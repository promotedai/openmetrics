package ai.promoted.metrics.logprocessor.common.job.hudi;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SYNC_AS_DATA_SOURCE_TABLE;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_CONDITIONAL_SYNC;
import static org.apache.hudi.table.catalog.CatalogOptions.CATALOG_PATH;
import static org.apache.hudi.table.catalog.CatalogOptions.DEFAULT_DATABASE;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.FeatureFlag;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.table.FlinkTableUtils;
import com.google.protobuf.GeneratedMessageV3;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.catalog.HoodieCatalog;
import org.apache.hudi.table.catalog.HoodieCatalogFactory;
import org.apache.hudi.util.HoodiePipeline;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Option;

public class HudiOutput implements FlinkSegment, AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(HudiOutput.class);
  private final BaseFlinkJob baseJob;

  // TODO More hudi options
  @Option(
      names = {"--hudiCatalogName"},
      description = "The name for Flink Hudi catalog.")
  public String hudiCatalogName;

  @Option(
      names = {"--hudiDefaultDatabaseName"},
      description = "The name for the default Hudi database.")
  public String hudiDefaultDatabaseName;

  @Option(
      names = {"--hudiBasePath"},
      description = "The base path for Hudi tables (data and catalog).")
  public String hudiBasePath;

  @FeatureFlag
  @Option(
      names = {"--enableHudiGlueSync"},
      description = "Whether to sync Hudi catalog to Glue. Default=true")
  public boolean enableHudiGlueSync = true;

  @FeatureFlag
  @Option(
      names = {"--enableMetadataTable"},
      negatable = true,
      description = "Whether to entable metadata table for Hudi. Default=false")
  public boolean enableMetadataTable = false;

  private String catalogBase;
  private HoodieCatalog catalog;

  public HudiOutput(BaseFlinkJob baseJob) {
    checkNotNull(baseJob, "Base job can't be null.");
    this.baseJob = baseJob;
  }

  public void open() throws IOException {
    catalogBase = hudiBasePath + "/catalog/";
    Configuration flinkConfiguration = new Configuration();
    flinkConfiguration.addAll(
        Configuration.fromMap(
            genDatabaseOptions(
                hudiDefaultDatabaseName + "_" + baseJob.jobLabel, Collections.emptyMap())));
    org.apache.hadoop.fs.FileSystem fs =
        FSUtils.getFs(hudiBasePath, HadoopConfigurations.getHadoopConf(flinkConfiguration));
    LOGGER.info("Create catalog dir: {}", catalogBase);
    fs.mkdirs(new org.apache.hadoop.fs.Path(catalogBase));
    catalog = new HoodieCatalog(hudiCatalogName, flinkConfiguration);
    catalog.open();
  }

  @Override
  public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return Collections.emptyList();
  }

  @Override
  public void validateArgs() {}

  public <T extends IndexedRecord> DataStreamSink<?> outputAvroToHudi(
      DataStream<T> input,
      String database,
      String tableName,
      List<String> pkExpressions,
      List<String> partitionExpressions,
      Map<String, String> extraOptions) {
    Class<T> clazz = input.getType().getTypeClass();
    Schema avroSchema = FlinkTableUtils.toAvroSchema(clazz);
    RowType rowType = (RowType) FlinkTableUtils.avroSchemaToDataType(avroSchema).getLogicalType();
    SingleOutputStreamOperator<RowData> rowDataStream =
        FlinkTableUtils.avroToRowDataStream(input, rowType);
    return buildHudiPipeline(
        rowDataStream,
        database,
        tableName,
        avroSchema,
        pkExpressions,
        partitionExpressions,
        extraOptions);
  }

  /** Output the avro input to the default hudi database. */
  public <T extends IndexedRecord> DataStreamSink<?> outputAvroToHudi(
      DataStream<T> input,
      String table,
      List<String> pkExpressions,
      List<String> partitionExpressions,
      Map<String, String> extraOptions) {
    return outputAvroToHudi(
        input,
        hudiDefaultDatabaseName + "_" + baseJob.jobLabel,
        table,
        pkExpressions,
        partitionExpressions,
        extraOptions);
  }

  public <T extends GeneratedMessageV3> DataStreamSink<?> outputProtoToHudi(
      DataStream<T> input,
      String database,
      String tableName,
      List<String> pkExpressions,
      List<String> partitionExpressions,
      Map<String, String> extraOptions) {
    Class<T> clazz = input.getType().getTypeClass();
    Schema avroSchema = FlinkTableUtils.protoToAvroSchema(clazz);
    RowType rowType = (RowType) FlinkTableUtils.avroSchemaToDataType(avroSchema).getLogicalType();
    SingleOutputStreamOperator<RowData> rowDataStream =
        FlinkTableUtils.protoToRowDataStream(input, rowType);
    return buildHudiPipeline(
        rowDataStream,
        database,
        tableName,
        avroSchema,
        pkExpressions,
        partitionExpressions,
        extraOptions);
  }

  /** Output the proto input to the default hudi database. */
  public <T extends GeneratedMessageV3> DataStreamSink<?> outputProtoToHudi(
      DataStream<T> input,
      String tableName,
      List<String> pkExpressions,
      List<String> partitionExpressions,
      Map<String, String> extraOptions) {
    return outputProtoToHudi(
        input,
        hudiDefaultDatabaseName + "_" + baseJob.jobLabel,
        tableName,
        pkExpressions,
        partitionExpressions,
        extraOptions);
  }

  private DataStreamSink<?> buildHudiPipeline(
      SingleOutputStreamOperator<RowData> rowDataStream,
      String databaseName,
      String tableName,
      Schema avroSchema,
      @Nullable List<String> pkExpressions,
      @Nullable List<String> partitionExpressions,
      Map<String, String> extraOptions) {
    final List<String> resolvedPkExpressions = new ArrayList<>();
    final List<String> resolvedPartitionExpressions = new ArrayList<>();
    final Map<String, String> resolvedExtraOptions = new HashMap<>(extraOptions);
    if (null == partitionExpressions || partitionExpressions.isEmpty()) {
      LOGGER.warn("No partition fields defined for table {}.", tableName);
      resolvedPartitionExpressions.add("");
    } else {
      resolvedPartitionExpressions.addAll(partitionExpressions);
    }
    if (null == pkExpressions || pkExpressions.isEmpty()) {
      LOGGER.warn(
          "No PK defined for table {}. "
              + "Will auto generate an UUID column for PK, and use insert only mode and bucket index.",
          tableName);
      resolvedPkExpressions.add("UUID() uuid");
      // TODO Bucket indexing uses customPartition which conflicts with unaligned checkpoint.
      //      resolvedExtraOptions.put(FlinkOptions.OPERATION.key(),
      // WriteOperationType.INSERT.value());
      // resolvedExtraOptions.put(FlinkOptions.INDEX_TYPE.key(), IndexType.BUCKET.name());
      // resolvedExtraOptions.put(FlinkOptions.INDEX_KEY_FIELD.key(), "uuid");
    } else {
      resolvedPkExpressions.addAll(pkExpressions);
    }
    Tuple3<DataStream<RowData>, ResolvedSchema, List<String>> streamTuple3 =
        extractNestedPkPartitionFields(
            rowDataStream, avroSchema, resolvedPkExpressions, resolvedPartitionExpressions);
    DataStream<RowData> streamWithPk = streamTuple3.f0;
    ResolvedSchema schemaWithPk = streamTuple3.f1;
    List<String> resolvedPartitionFields = streamTuple3.f2;

    Map<String, String> tableOptions =
        genTableOptions(
            databaseName,
            tableName,
            // The avro schema could have been updated with extracted nested PK fields
            FlinkTableUtils.logicalTypeToAvroSchema(
                    schemaWithPk.toPhysicalRowDataType().getLogicalType())
                .toString(),
            schemaWithPk.getPrimaryKey().get().getColumns(),
            resolvedPartitionFields,
            resolvedExtraOptions);
    createHudiFlinkTableIfNotExist(
        databaseName, tableName, schemaWithPk, resolvedPartitionFields, tableOptions);
    HoodiePipeline.Builder builder = HoodiePipeline.builder(tableName).options(tableOptions);
    setSchemaForBuilder(builder, schemaWithPk, resolvedPartitionFields);
    return builder.sink(streamWithPk, false);
  }

  /**
   * Returns a tuple3 of
   *
   * <ol>
   *   <li>The new DataStream with all PK fields extracted from nested row types;
   *   <li>The new schema;
   *   <li>The extracted partition fields.
   * </ol>
   */
  private Tuple3<DataStream<RowData>, ResolvedSchema, List<String>> extractNestedPkPartitionFields(
      SingleOutputStreamOperator<RowData> rowDataStream,
      Schema avroSchema,
      List<String> pkExpressions,
      List<String> partitionExpressions) {

    // Register the original RowData stream to Flink table env
    StreamTableEnvironment tableEnv =
        StreamTableEnvironment.create(rowDataStream.getExecutionEnvironment());
    String tempViewName = "temp_view_" + UUID.randomUUID().toString().replaceAll("-", "");
    FlinkTableUtils.registerView(tableEnv, rowDataStream, avroSchema, tempViewName);

    List<String> existingColumns = new ArrayList<>();
    avroSchema
        .getFields()
        .forEach(
            field -> {
              if (pkExpressions.contains(field.name())
                  || partitionExpressions.contains(field.name())) {
                existingColumns.add(field.name());
              }
            });

    // The selected columns will be (PK, partition_field, remaining fields).
    // E.g., for schema (platform_id, request) with PK (platform_id, request.request_id) and
    // partition_field request.Timing.event_api_timestamp,
    // the selected columns will be
    // (platform_id, request.request_id, request.Timing.event_api_timestamp, request).
    // The result schema will be (platform_id, request_id, event_api_timestamp, request).
    List<String> selectedColumns = new ArrayList<>(pkExpressions);
    selectedColumns.addAll(partitionExpressions);
    avroSchema.getFields().stream()
        .filter(field -> !existingColumns.contains(field.name()))
        .forEach(field -> selectedColumns.add("`" + field.name() + "`"));
    String sql = "SELECT " + String.join(", ", selectedColumns) + " FROM " + tempViewName;
    Table resultTable = tableEnv.sqlQuery(sql);
    ResolvedSchema resultSchema = resultTable.getResolvedSchema();

    ResolvedSchema resultSchemaWithPk =
        setPk(
            resultSchema,
            // The nested PK fields will be renamed (e.g., request.request_id to request_id).
            // Since PK are the first n fields (where n = pkColumns.size()), just get their names.
            resultSchema.getColumns().subList(0, pkExpressions.size()).stream()
                .map(Column::getName)
                .collect(Collectors.toList()));
    List<String> renamedPartitions =
        resultSchema
            .getColumns()
            .subList(pkExpressions.size(), pkExpressions.size() + partitionExpressions.size())
            .stream()
            .map(Column::getName)
            .collect(Collectors.toList());
    DataType resultDataType = resultSchemaWithPk.toPhysicalRowDataType();
    LOGGER.info("Schema with primary key: {}", resultSchemaWithPk.toString());

    // After extracting nested PK and partition columns, transform the result table back to
    // DataStream<RowData> for Hudi to use.
    DataStream<RowData> resultStream =
        tableEnv.toDataStream(
            resultTable, DataTypes.of(InternalTypeInfo.of(resultDataType.getLogicalType())));
    return Tuple3.of(resultStream, resultSchemaWithPk, renamedPartitions);
  }

  private void setSchemaForBuilder(
      HoodiePipeline.Builder builder,
      ResolvedSchema resolvedSchemaWithPk,
      List<String> partitionFields) {
    for (Column column : resolvedSchemaWithPk.getColumns()) {
      builder.column(column.toString());
    }
    builder.partition(partitionFields.toArray(new String[0]));
    resolvedSchemaWithPk
        .getPrimaryKey()
        .ifPresent(
            constraint ->
                builder.pk(
                    constraint.getColumns().stream()
                        .map(EncodingUtils::escapeIdentifier)
                        .collect(Collectors.joining(", "))));
  }

  private ResolvedSchema setPk(ResolvedSchema resolvedSchema, List<String> pkColumns) {
    UniqueConstraint pk = pkColumns.isEmpty() ? null : UniqueConstraint.primaryKey("pk", pkColumns);
    // Set the PK fields to be not nullable
    List<Column> columnList =
        resolvedSchema.getColumns().stream()
            .map(
                column -> {
                  if (pkColumns.contains(column.getName())
                      && column.getDataType().getLogicalType().isNullable()) {
                    return column.copy(column.getDataType().notNull());
                  } else {
                    return column;
                  }
                })
            .collect(Collectors.toList());
    return new ResolvedSchema(columnList, Collections.emptyList(), pk);
  }

  private Map<String, String> genTableOptions(
      String database,
      String table,
      String avroSchema,
      List<String> pkFields,
      List<String> partitionFields,
      Map<String, String> extraOptions) {
    Map<String, String> tableOptions = genDatabaseOptions(database, Collections.emptyMap());
    // Disable this to reduce Hive sync entity size. We hit the following exception.
    // com.amazonaws.services.glue.model.InvalidInputException: Entity size has exceeded the maximum
    // allowed size. (Service: AWSGlue; Status Code: 400; Error Code: InvalidInputException;
    tableOptions.put(HIVE_SYNC_AS_DATA_SOURCE_TABLE.key(), "false");

    String tablePath = String.join("/", hudiBasePath, database, table);
    tableOptions.put(FlinkOptions.PATH.key(), tablePath);
    tableOptions.put(FlinkOptions.SOURCE_AVRO_SCHEMA.key(), avroSchema);

    // TTL for state
    tableOptions.put(FlinkOptions.INDEX_STATE_TTL.key(), "0.3");
    tableOptions.put(FlinkOptions.INDEX_KEY_FIELD.key(), String.join(",", pkFields));
    // Partitioning
    tableOptions.put(FlinkOptions.PARTITION_PATH_FIELD.key(), String.join(",", partitionFields));
    //    tableOptions.put(FlinkOptions.KEYGEN_CLASS_NAME.key(),
    // TimestampBasedAvroKeyGenerator.class.getName());
    tableOptions.put(KeyGeneratorOptions.Config.TIMESTAMP_TYPE_FIELD_PROP, "EPOCHMILLISECONDS");
    tableOptions.put(KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, "yyyy-MM-dd-hh");
    tableOptions.put(KeyGeneratorOptions.Config.TIMESTAMP_TIMEZONE_FORMAT_PROP, "UTC+0:00");

    // Parquet
    tableOptions.put(PARQUET_COMPRESSION_CODEC_NAME.key(), "snappy");

    // Clustering
    //    tableOptions.put(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED.key(), "true");

    // Glue sync
    if (enableHudiGlueSync) {
      LOGGER.info("Enable syncing catalog to glue.");
      tableOptions.put(META_SYNC_CONDITIONAL_SYNC.key(), "true");
      tableOptions.put(FlinkOptions.HIVE_SYNC_ENABLED.key(), "true");
      tableOptions.put(FlinkOptions.HIVE_SYNC_MODE.key(), "glue");
      tableOptions.put(FlinkOptions.HIVE_SYNC_DB.key(), database);
      tableOptions.put(FlinkOptions.HIVE_SYNC_TABLE.key(), table);
      tableOptions.put(FlinkOptions.HIVE_SYNC_SKIP_RO_SUFFIX.key(), "true");
    }
    if (null != extraOptions) {
      tableOptions.putAll(extraOptions);
    }
    return tableOptions;
  }

  private Map<String, String> genDatabaseOptions(
      String defaultDatabaseName, @Nullable Map<String, String> extraOptions) {
    checkNotNull(defaultDatabaseName, "The default database name can't be null!");
    final Map<String, String> options = new HashMap<>();
    options.put(CommonCatalogOptions.CATALOG_TYPE.key(), HoodieCatalogFactory.IDENTIFIER);
    options.put(CATALOG_PATH.key(), catalogBase);
    options.put(DEFAULT_DATABASE.key(), defaultDatabaseName);
    options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());

    if (enableMetadataTable) {
      options.put(FlinkOptions.METADATA_ENABLED.key(), "true");
      int metadataCompactionDeltaCommits = (int) (600000 / baseJob.checkpointInterval.toMillis());
      options.put(
          FlinkOptions.METADATA_COMPACTION_DELTA_COMMITS.key(),
          String.valueOf(Math.max(metadataCompactionDeltaCommits, 2)));
      options.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
      options.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER.key(), "true");
    }
    options.put(FlinkOptions.COMPACTION_TRIGGER_STRATEGY.key(), FlinkOptions.NUM_OR_TIME);
    options.put(FlinkOptions.COMPACTION_DELTA_SECONDS.key(), "600"); // Every 10 minutes
    options.put(FlinkOptions.HIVE_STYLE_PARTITIONING.key(), "true");
    if (null != extraOptions) {
      options.putAll(extraOptions);
    }
    return options;
  }

  private synchronized void createHudiFlinkTableIfNotExist(
      String database,
      String table,
      ResolvedSchema resolvedSchemaWithPk,
      List<String> partitionKeys,
      Map<String, String> options) {
    org.apache.flink.table.api.Schema flinkSchema =
        org.apache.flink.table.api.Schema.newBuilder()
            .fromResolvedSchema(resolvedSchemaWithPk)
            .build();
    try {
      if (!catalog.databaseExists(database)) {
        LOGGER.info("Create hudi database {}", database);
        catalog.createDatabase(
            database,
            new CatalogDatabaseImpl(Collections.emptyMap(), "comment for database"),
            true);
      }
      ObjectPath tablePath = new ObjectPath(database, table);
      if (!catalog.tableExists(tablePath)) {
        LOGGER.info("Create hudi table {}", table);
        // TODO validate the existing table matches the requested one
        catalog.createTable(
            tablePath,
            new ResolvedCatalogTable(
                CatalogTable.of(flinkSchema, database + "." + table, partitionKeys, options),
                resolvedSchemaWithPk),
            true);
      }
    } catch (TableAlreadyExistException
        | DatabaseNotExistException
        | DatabaseAlreadyExistException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    catalog.close();
  }
}
