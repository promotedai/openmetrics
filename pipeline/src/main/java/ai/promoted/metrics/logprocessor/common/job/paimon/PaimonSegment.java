package ai.promoted.metrics.logprocessor.common.job.paimon;

import static ai.promoted.metrics.logprocessor.common.util.TableUtil.fullTableName;
import static ai.promoted.metrics.logprocessor.common.util.TableUtil.genTableOptionsStr;
import static ai.promoted.metrics.logprocessor.common.util.TableUtil.getLabeledDatabaseName;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkTableJob;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.table.FlinkTableUtils;
import ai.promoted.metrics.logprocessor.common.util.TableUtil;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.GeneratedMessageV3;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

public class PaimonSegment implements FlinkSegment {
  private static final Logger LOGGER = LogManager.getLogger(PaimonSegment.class);

  @CommandLine.Option(
      names = {"--paimonBasePath"},
      description = "The base path for Paimon tables (data and catalog).")
  public String paimonBasePath;

  @CommandLine.Option(
      names = {"--paimonCatalogName"},
      description = "The name for Flink Paimon catalog. Default=\"paimon\"")
  public String paimonCatalogName = "paimon";

  @CommandLine.Option(
      names = {"--paimonDefaultDatabaseName"},
      description = "The name for the default Paimon database. Default=\"default\"")
  public String paimonDefaultDatabaseName = "default";

  @CommandLine.Option(
      names = {"--paimonGlobalOptions"},
      description =
          "Map of global options for Paimon tables. Will overwrite the default configs. Default=Empty")
  public Map<String, String> paimonGlobalOptions = Collections.emptyMap();

  @CommandLine.Option(
      names = {"--paimonTableOptions"},
      description =
          "Map of table specific options for Paimon. Will overwrite the default and global configs."
              + " The key must be the format of '{database}.{table}.{option-key}'. Default=Empty")
  public Map<String, String> paimonTableOptions = Collections.emptyMap();

  @CommandLine.Option(
      names = {"--paimonAlterTable"},
      description =
          "Whether to alter a table's properties with the provided options if the table already exists."
              + " The key must be the format of '{database}.{table}' and the value is boolean. Default=Empty")
  public Map<String, Boolean> paimonAlterTable = Collections.emptyMap();

  private StreamTableEnvironment tEnv;

  private BaseFlinkJob baseFlinkJob;
  private StreamStatementSet statementSet;

  public PaimonSegment(BaseFlinkJob baseFlinkJob, @Nullable StreamTableEnvironment tEnv) {
    this.baseFlinkJob = baseFlinkJob;
    if (null != tEnv) {
      setStreamTableEnvironment(tEnv);
    }
  }

  public PaimonSegment(BaseFlinkTableJob baseFlinkJob) {
    this(baseFlinkJob, null);
  }

  public StreamStatementSet getStatementSet() {
    return statementSet;
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return Collections.emptySet();
  }

  public void setStreamTableEnvironment(StreamTableEnvironment tEnv) {
    this.tEnv = tEnv;
    this.statementSet = tEnv.createStatementSet();
  }

  @Override
  public void validateArgs() {}

  public synchronized void createPaimonCatalog() {
    Preconditions.checkNotNull(tEnv, "tEnv");
    if (tEnv.getCatalog(paimonCatalogName).isEmpty()) {
      String createCatalogSql =
          "CREATE CATALOG `"
              + paimonCatalogName
              + "` WITH ("
              + "'type' = 'paimon',"
              + "'warehouse' = '"
              + paimonBasePath
              + "',"
              + "'default-database' = '"
              + getLabeledDatabaseName(paimonDefaultDatabaseName, baseFlinkJob.getJobLabel())
              + "')";
      executeSqlAndValidate(createCatalogSql);
    }
  }

  public <T extends IndexedRecord> void writeAvroToPaimon(
      DataStream<T> input,
      String database,
      String tableName,
      @Nullable List<String> fieldsExtractions,
      @Nullable List<String> partialPkFields,
      @Nullable List<String> partitionFields,
      Map<String, String> extraOptions) {
    Class<T> clazz = input.getType().getTypeClass();
    Schema avroSchema = FlinkTableUtils.toAvroSchema(clazz);
    RowType rowType = (RowType) FlinkTableUtils.avroSchemaToDataType(avroSchema).getLogicalType();
    SingleOutputStreamOperator<RowData> rowDataStream =
        FlinkTableUtils.avroToRowDataStream(input, rowType);
    Map<String, String> mergedExtraOptions = new HashMap<>(extraOptions);
    writePaimonTable(
        rowDataStream,
        database,
        tableName,
        avroSchema,
        null != fieldsExtractions ? fieldsExtractions : Collections.emptyList(),
        null != partialPkFields ? partialPkFields : Collections.emptyList(),
        null != partitionFields ? partitionFields : Collections.emptyList(),
        mergedExtraOptions);
  }

  public <T extends GeneratedMessageV3> void writeProtoToPaimon(
      DataStream<T> input,
      String database,
      String tableName,
      @Nullable List<String> fieldsExtraction,
      @Nullable List<String> partialPkFields,
      @Nullable List<String> partitionFields,
      Map<String, String> extraOptions) {
    Class<T> clazz = input.getType().getTypeClass();
    Schema avroSchema = FlinkTableUtils.protoToAvroSchema(clazz);
    RowType rowType = (RowType) FlinkTableUtils.avroSchemaToDataType(avroSchema).getLogicalType();
    SingleOutputStreamOperator<RowData> rowDataStream =
        FlinkTableUtils.protoToRowDataStream(input, rowType);
    Map<String, String> mergedExtraOptions = new HashMap<>(extraOptions);
    writePaimonTable(
        rowDataStream,
        database,
        tableName,
        avroSchema,
        null != fieldsExtraction ? fieldsExtraction : Collections.emptyList(),
        null != partialPkFields ? partialPkFields : Collections.emptyList(),
        null != partitionFields ? partitionFields : Collections.emptyList(),
        mergedExtraOptions);
  }

  private void writePaimonTable(
      SingleOutputStreamOperator<RowData> rowDataStream,
      String databaseName,
      String tableName,
      Schema avroSchema,
      List<String> fieldsExtractions,
      List<String> partialPkFields,
      List<String> partitionFields,
      Map<String, String> extraOptions) {

    String labeledDatabase =
        TableUtil.getLabeledDatabaseName(databaseName, baseFlinkJob.getJobLabel());
    createPaimonCatalog();
    // The real PK should be partial PK + partition expressions
    final List<String> resolvedPkFields = new ArrayList<>();
    final List<String> resolvedPartitionFields = new ArrayList<>();
    final Map<String, String> resolvedOtherOptions =
        resolveExtraOptions(databaseName, tableName, extraOptions);

    if (null == partialPkFields || partialPkFields.isEmpty()) {
      LOGGER.warn("No PK defined for table {}. ", tableName);
    } else {
      resolvedPkFields.addAll(partialPkFields);
    }
    if (null == partitionFields || partitionFields.isEmpty()) {
      LOGGER.warn("No partition fields defined for table {}.", tableName);
    } else {
      resolvedPartitionFields.addAll(partitionFields);
      // For Paimon tables, if PK is defined, it must include the partition fields.
      if (!resolvedPkFields.isEmpty()) {
        for (String partitionField : partitionFields) {
          if (!resolvedPkFields.contains(partitionField)) {
            resolvedPkFields.add(partitionField);
          }
        }
      }
    }

    Tuple2<String, RowType> tempViewInfo =
        registerStreamAndExtractFields(
            rowDataStream,
            tableName,
            avroSchema,
            fieldsExtractions,
            resolvedPkFields,
            resolvedPartitionFields);
    // If resolvedFields is not null, it must include the partition fields
    int notNullFieldsNum =
        !resolvedPkFields.isEmpty() ? resolvedPkFields.size() : resolvedPartitionFields.size();
    List<RowType.RowField> paimonTableFields = new ArrayList<>(tempViewInfo.f1.getFields().size());
    for (int i = 0; i < tempViewInfo.f1.getFields().size(); ++i) {
      RowType.RowField rowField = tempViewInfo.f1.getFields().get(i);
      if (i < notNullFieldsNum) {
        paimonTableFields.add(
            new RowType.RowField(rowField.getName(), rowField.getType().copy(false)));
      } else {
        paimonTableFields.add(rowField);
      }
    }
    String fieldsDeclaration =
        paimonTableFields.stream()
            .map(RowType.RowField::asSummaryString)
            .collect(Collectors.joining(","));

    createPaimonDatabase(labeledDatabase);

    String fullTableName = fullTableName(paimonCatalogName, labeledDatabase, tableName);

    String createPaimonTableSql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s (%s) WITH (%s)",
            fullTableName,
            fieldsDeclaration,
            genTableOptionStr(resolvedPkFields, resolvedPartitionFields, resolvedOtherOptions));
    executeSqlAndValidate(createPaimonTableSql);
    String otherOptionsStr = genTableOptionsStr(resolvedOtherOptions);
    if (shouldAlterTable(databaseName, tableName)) {
      String alterTableSql =
          String.format("ALTER TABLE %s SET (%s)", fullTableName, otherOptionsStr);
      executeSqlAndValidate(alterTableSql);
    }

    String insertIntoPaimonTableSql =
        String.format(
            "INSERT INTO %s /*+ OPTIONS(%s) */ SELECT * FROM %s",
            fullTableName, otherOptionsStr, tempViewInfo.f0);
    LOGGER.info(insertIntoPaimonTableSql);
    statementSet.addInsertSql(insertIntoPaimonTableSql);
  }

  public Map<String, String> resolveExtraOptions(
      String databaseName, String tableName, Map<String, String> extraOptions) {
    Map<String, String> resolvedOtherOptions = new HashMap<>(getDefaultTableOptions());
    resolvedOtherOptions.putAll(extraOptions);
    resolvedOtherOptions.putAll(paimonGlobalOptions);
    resolvedOtherOptions.putAll(genTableSpecificOptions(databaseName, tableName));
    return resolvedOtherOptions;
  }

  private void executeSqlAndValidate(String sql) {
    LOGGER.info(sql);
    try {
      tEnv.executeSql(sql).await();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public String genTableOptionStr(
      List<String> resolvedPkFields,
      List<String> resolvedPartitionFields,
      Map<String, String> otherOptions) {
    String pkOption =
        resolvedPkFields.isEmpty()
            ? ""
            : String.format("'primary-key' = '%s', ", String.join(",", resolvedPkFields));
    String partitionOption =
        resolvedPartitionFields.isEmpty()
            ? ""
            : String.format("'partition' = '%s', ", String.join(",", resolvedPartitionFields));
    return pkOption + partitionOption + genTableOptionsStr(otherOptions);
  }

  private boolean shouldAlterTable(String database, String table) {
    String databaseAndTableName = TableUtil.databaseAndTableName(database, table);
    return paimonAlterTable.getOrDefault(databaseAndTableName, false);
  }

  /**
   * Register the datastream, extract the nested fields and put primary key/partition keys fields to
   * the beginning of the table schema.
   */
  private Tuple2<String, RowType> registerStreamAndExtractFields(
      SingleOutputStreamOperator<RowData> rowDataStream,
      String tableName,
      Schema avroSchema,
      List<String> fieldsExtractions,
      List<String> pkFields,
      List<String> partitionFields) {
    // Register the original RowData stream to Flink table env
    String sourceTempView =
        String.format("src_tmp_%s_%s", tableName, UUID.randomUUID().toString().replaceAll("-", ""));
    FlinkTableUtils.registerView(tEnv, rowDataStream, avroSchema, sourceTempView);
    // add the extracted expressions
    List<String> extractionSelectionColumns = new ArrayList<>(fieldsExtractions);
    // add the existing fields
    avroSchema
        .getFields()
        .forEach(field -> extractionSelectionColumns.add("`" + field.name() + "`"));
    String fieldsExtractionSql =
        "SELECT " + String.join(", ", extractionSelectionColumns) + " FROM " + sourceTempView;

    Table columnExtractedTable = tEnv.sqlQuery(fieldsExtractionSql);
    String columnExtractedView =
        String.format(
            "extracted_tmp_%s_%s", tableName, UUID.randomUUID().toString().replaceAll("-", ""));
    tEnv.createTemporaryView(columnExtractedView, columnExtractedTable);
    RowType columnExtractedRowType =
        (RowType) columnExtractedTable.getResolvedSchema().toPhysicalRowDataType().getLogicalType();

    // Add backticks to all column names in case keywords are used.
    List<String> extractedFields = new ArrayList<>();
    for (int i = 0; i < fieldsExtractions.size(); ++i) {
      extractedFields.add("`" + columnExtractedRowType.getFields().get(i).getName() + "`");
    }
    List<String> pkOrPartitionFields = pkFields.isEmpty() ? partitionFields : pkFields;
    LinkedHashSet<String> resultSelectionColumns =
        pkOrPartitionFields.stream()
            .map(f -> "`" + f + "`")
            .collect(Collectors.toCollection(LinkedHashSet::new));
    resultSelectionColumns.addAll(extractedFields);
    for (RowType.RowField rowField : columnExtractedRowType.getFields()) {
      if (!resultSelectionColumns.contains(rowField.getName())) {
        resultSelectionColumns.add("`" + rowField.getName() + "`");
      }
    }
    String resultView =
        String.format(
            "result_tmp_%s_%s", tableName, UUID.randomUUID().toString().replaceAll("-", ""));
    String reorderSelectionSql =
        "SELECT " + String.join(", ", resultSelectionColumns) + " FROM " + columnExtractedView;
    LOGGER.info(reorderSelectionSql);
    Table resultTable = tEnv.sqlQuery(reorderSelectionSql);
    tEnv.createTemporaryView(resultView, resultTable);

    return Tuple2.of(
        resultView,
        (RowType) resultTable.getResolvedSchema().toPhysicalRowDataType().getLogicalType());
  }

  public CloseableIterator<Row> createPaimonDatabase(String database) {
    String createDatabaseSql =
        String.format("CREATE DATABASE IF NOT EXISTS `%s`.`%s`", paimonCatalogName, database);
    LOGGER.info(createDatabaseSql);
    return tEnv.executeSql(createDatabaseSql).collect();
  }

  private Map<String, String> getDefaultTableOptions() {
    return ImmutableMap.of("file.format", "orc");
  }

  private Map<String, String> genTableSpecificOptions(String database, String table) {
    String prefix = TableUtil.databaseAndTableName(database, table);
    return paimonTableOptions.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(prefix))
        .collect(
            Collectors.toMap(e -> e.getKey().substring(prefix.length() + 1), Map.Entry::getValue));
  }
}
