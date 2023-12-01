package ai.promoted.metrics.logprocessor.common.table;

import ai.promoted.metrics.logprocessor.common.avro.PromotedProtobufData;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSourceProvider;
import ai.promoted.metrics.logprocessor.common.job.ValidatedDataSourceProvider;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetricsTableCatalog {
  // TODO catalogs are customer-env specific
  private static final Logger LOGGER = LogManager.getLogger(MetricsTableCatalog.class);
  private static final String DEFAULT_FS_AVRO_DATABASE = "fs_avro";
  private static final String DEFAULT_CATALOG_SUFFIX = "metrics_default";
  private static final String PAIMON_CATALOG_SUFFIX = "metrics_paimon";
  private final String kafkaConsumerGroup;
  private final String jobLabel;
  private final String platformEnv;
  private final String defaultCatalogName;
  private final Catalog defaultCatalog;
  private final Map<String, FlatOutputKafkaSourceProvider> flatOutputKafkaSourceProviderMap =
      new HashMap<>();
  private final Map<String, ValidatedDataSourceProvider> validatedSourceProviderMap =
      new HashMap<>();

  private Map<String, String> fsAvroPathSchemas = Collections.emptyMap();
  private List<String> paimonCatalogPaths = Collections.emptyList();

  public MetricsTableCatalog(
      @Nullable String platformEnv, String jobLabel, String kafkaConsumerGroup) {
    this.kafkaConsumerGroup = kafkaConsumerGroup;
    this.platformEnv = platformEnv;
    this.jobLabel = jobLabel;
    if (StringUtil.isBlank(platformEnv)) {
      this.defaultCatalogName = String.format("%s_%s", DEFAULT_CATALOG_SUFFIX, jobLabel);
    } else {
      this.defaultCatalogName =
          String.format("%s_%s_%s", platformEnv, DEFAULT_CATALOG_SUFFIX, jobLabel);
    }
    this.defaultCatalog = new GenericInMemoryCatalog(defaultCatalogName);
  }

  public void addFlatOutputSourceProvider(
      String database, FlatOutputKafkaSourceProvider flatOutputSourceProvider) {
    flatOutputKafkaSourceProviderMap.put(database, flatOutputSourceProvider);
  }

  public void addValidatedSourceProvider(
      String database, ValidatedDataSourceProvider validatedSourceProvider) {
    validatedSourceProviderMap.put(database, validatedSourceProvider);
  }

  public void setFsAvroPathSchemas(Map<String, String> fsAvroPathSchemas) {
    this.fsAvroPathSchemas = new HashMap<>(fsAvroPathSchemas);
  }

  public void setPaimonCatalogPaths(List<String> paimonCatalogPaths) {
    this.paimonCatalogPaths = new ArrayList<>(paimonCatalogPaths);
  }

  private String genFullTableName(String catalog, String database, String table) {
    return String.format("`%s`.`%s`.`%s`", catalog, database, table);
  }

  public List<String> registerMetricsTables(
      StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
    tableEnv.registerCatalog(defaultCatalogName, defaultCatalog);
    ImmutableList.Builder<String> tableListBuilder = ImmutableList.builder();
    tableListBuilder.addAll(registerKafkaTables(env, tableEnv));
    tableListBuilder.addAll(registerPaimonTables(tableEnv));
    try {
      tableListBuilder.addAll(registerFileSystemAvroTables(tableEnv));
    } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    tableEnv.executeSql("use catalog `default_catalog`");
    return tableListBuilder.build();
  }

  private List<String> registerKafkaTables(
      StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
    ImmutableList.Builder<String> tableListBuilder = ImmutableList.builder();
    for (Map.Entry<String, FlatOutputKafkaSourceProvider> entry :
        flatOutputKafkaSourceProviderMap.entrySet()) {
      String flatOutputDatabase = entry.getKey();
      LOGGER.info("Register FlatOutputKafkaSource tables on database {}", flatOutputDatabase);
      createDatabase(defaultCatalog, flatOutputDatabase, "The database for direct flat output");
      tableListBuilder.add(
          registerProtoStream(
              tableEnv,
              entry.getValue().getJoinedImpressionSource(env, kafkaConsumerGroup),
              defaultCatalogName,
              flatOutputDatabase,
              "joined_impression"));
      tableListBuilder.add(
          registerProtoStream(
              tableEnv,
              entry.getValue().getAttributedActionSource(env, kafkaConsumerGroup),
              defaultCatalogName,
              flatOutputDatabase,
              "attributed_action"));
    }
    for (Map.Entry<String, ValidatedDataSourceProvider> entry :
        validatedSourceProviderMap.entrySet()) {
      String directValidatedDatabase = entry.getKey();
      LOGGER.info("Register ValidatedKafkaSource tables on database {}, ", directValidatedDatabase);
      createDatabase(defaultCatalog, directValidatedDatabase, "The database for validated event");
      tableListBuilder.addAll(
          registerValidatedSourceTables(env, tableEnv, entry.getValue(), directValidatedDatabase));
    }
    return tableListBuilder.build();
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  List<String> registerFileSystemAvroTables(TableEnvironment tableEnv)
      throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
    if (!fsAvroPathSchemas.isEmpty()) {
      createDatabase(
          defaultCatalog, DEFAULT_FS_AVRO_DATABASE, "The database for FileSystem avro tables");
    }
    ImmutableList.Builder<String> tableListBuilder = ImmutableList.builder();
    Map<String, Integer> tableNameGenMap = new HashMap<>();
    for (Map.Entry<String, String> entry : fsAvroPathSchemas.entrySet()) {
      Class<?> schemaClass = Class.forName(entry.getValue());
      String simpleSchemaName = schemaClass.getSimpleName();
      Schema avroSchema;
      if (GeneratedMessageV3.class.isAssignableFrom(schemaClass)) {
        // Proto Schema
        avroSchema = PromotedProtobufData.get().getSchema(schemaClass);
      } else if (IndexedRecord.class.isAssignableFrom(schemaClass)) {
        // Avro Schema
        avroSchema = (Schema) schemaClass.getField("SCHEMA$").get(null);
      } else {
        throw new IllegalArgumentException("Unsupported schema class " + entry.getValue());
      }
      DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema.toString());
      String fieldsDeclaration =
          ((RowType) dataType.getLogicalType())
              .getFields().stream()
                  .map(RowType.RowField::asSummaryString)
                  .collect(Collectors.joining(","));
      int schemaCount;
      if (tableNameGenMap.containsKey(simpleSchemaName)) {
        schemaCount = tableNameGenMap.get(simpleSchemaName);
        tableNameGenMap.put(simpleSchemaName, schemaCount + 1);
      } else {
        schemaCount = 0;
        tableNameGenMap.put(schemaClass.getSimpleName(), 1);
      }
      String fullTableName =
          genFullTableName(
              defaultCatalogName,
              DEFAULT_FS_AVRO_DATABASE,
              String.format("%s_%s", simpleSchemaName, schemaCount));
      // Use our own suffixedfilesystem connector and promoted-avro format to accept an extra Avro
      // schema in table options.
      String createTableSql =
          String.format(
              "CREATE TABLE %s (%s) WITH ('connector' = 'suffixedfilesystem', 'path' = '%s', 'format' = 'promoted-avro', 'promoted-avro.schema' = '%s')",
              fullTableName, fieldsDeclaration, entry.getKey(), avroSchema);
      tableEnv.executeSql(createTableSql);
      tableListBuilder.add(fullTableName);
    }
    return tableListBuilder.build();
  }

  private List<String> registerValidatedSourceTables(
      StreamExecutionEnvironment env,
      StreamTableEnvironment tableEnv,
      ValidatedDataSourceProvider validatedSourceProvider,
      String database) {
    ImmutableList.Builder<String> tableListBuilder = ImmutableList.builder();
    SingleOutputStreamOperator<? extends GeneratedMessageV3> protoSource =
        validatedSourceProvider.getViewSource(env, kafkaConsumerGroup);
    if (null != protoSource) {
      tableListBuilder.add(
          registerProtoStream(tableEnv, protoSource, defaultCatalogName, database, "view"));
    }

    protoSource = validatedSourceProvider.getDiagnosticsSource(env, kafkaConsumerGroup);
    if (null != protoSource) {
      tableListBuilder.add(
          registerProtoStream(tableEnv, protoSource, defaultCatalogName, database, "diagnostics"));
    }

    protoSource = validatedSourceProvider.getImpressionSource(env, kafkaConsumerGroup);
    if (null != protoSource) {
      tableListBuilder.add(
          registerProtoStream(tableEnv, protoSource, defaultCatalogName, database, "impression"));
    }

    protoSource = validatedSourceProvider.getCohortMembershipSource(env, kafkaConsumerGroup);
    if (null != protoSource) {
      tableListBuilder.add(
          registerProtoStream(
              tableEnv, protoSource, defaultCatalogName, database, "cohort_membership"));
    }

    protoSource = validatedSourceProvider.getActionSource(env, kafkaConsumerGroup);
    if (null != protoSource) {
      tableListBuilder.add(
          registerProtoStream(tableEnv, protoSource, defaultCatalogName, database, "action"));
    }

    protoSource = validatedSourceProvider.getDeliveryLogSource(env, kafkaConsumerGroup);
    if (null != protoSource) {
      tableListBuilder.add(
          registerProtoStream(tableEnv, protoSource, defaultCatalogName, database, "delivery_log"));
    }

    protoSource = validatedSourceProvider.getRetainedUserSource(env, kafkaConsumerGroup);

    if (null != protoSource) {
      tableListBuilder.add(
          registerProtoStream(
              tableEnv, protoSource, defaultCatalogName, database, "retained_user"));
    }
    SingleOutputStreamOperator<? extends IndexedRecord> avroSource =
        validatedSourceProvider.getLogUserUserSource(env, kafkaConsumerGroup);
    if (null != avroSource) {
      tableListBuilder.add(
          registerAvroStream(tableEnv, avroSource, defaultCatalogName, database, "log_user_user"));
    }
    return tableListBuilder.build();
  }

  List<String> registerPaimonTables(final TableEnvironment tableEnv) {
    ImmutableList.Builder<String> tableListBuilder = ImmutableList.builder();
    for (String paimonCatalogPath : paimonCatalogPaths) {
      if (!StringUtil.isBlank(paimonCatalogPath)) {
        LOGGER.info("Register Paimon Catalog from {}", paimonCatalogPath);
        String paimonCatalogName;
        if (StringUtil.isBlank(platformEnv)) {
          paimonCatalogName = String.format("%s_%s", PAIMON_CATALOG_SUFFIX, jobLabel);
        } else {
          paimonCatalogName =
              String.format("%s_%s_%s", platformEnv, PAIMON_CATALOG_SUFFIX, jobLabel);
        }
        String createCatalogSql =
            "CREATE CATALOG "
                + paimonCatalogName
                + " WITH ("
                + "'type' = 'paimon',"
                + "'warehouse' = '"
                + paimonCatalogPath
                + "')";
        tableEnv.executeSql(createCatalogSql);
        tableEnv.executeSql("Use catalog " + paimonCatalogName);
        Iterator<Row> databaseRowIterator = tableEnv.executeSql("show databases").collect();
        ImmutableList.Builder<String> paimonDatabaseList = ImmutableList.builder();
        databaseRowIterator.forEachRemaining(
            row -> paimonDatabaseList.add((String) row.getField(0)));
        paimonDatabaseList
            .build()
            .forEach(
                db -> {
                  tableEnv.executeSql("use `" + db + "`");
                  tableEnv
                      .executeSql("show tables")
                      .collect()
                      .forEachRemaining(
                          row ->
                              tableListBuilder.add(
                                  String.format(
                                      "`%s`.`%s`.`%s`", paimonCatalogName, db, row.getField(0))));
                });
      }
    }
    return tableListBuilder.build();
  }

  private void createDatabase(Catalog catalog, String databaseName, String comment) {
    CatalogDatabase database = new CatalogDatabaseImpl(new HashMap<>(), comment);
    try {
      catalog.createDatabase(databaseName, database, true);
    } catch (DatabaseAlreadyExistException e) {
      throw new RuntimeException(e);
    }
  }

  private String registerProtoStream(
      StreamTableEnvironment tableEnv,
      SingleOutputStreamOperator<? extends GeneratedMessageV3> protoStream,
      String catalog,
      String database,
      String table) {
    String fullTableName = genFullTableName(catalog, database, table);
    FlinkTableUtils.registerProtoView(tableEnv, protoStream, fullTableName);
    return fullTableName;
  }

  private String registerAvroStream(
      StreamTableEnvironment tableEnv,
      SingleOutputStreamOperator<? extends IndexedRecord> avroStream,
      String catalog,
      String database,
      String table) {
    String fullTableName = genFullTableName(catalog, database, table);
    FlinkTableUtils.registerAvroView(tableEnv, avroStream, fullTableName);
    return fullTableName;
  }
}
