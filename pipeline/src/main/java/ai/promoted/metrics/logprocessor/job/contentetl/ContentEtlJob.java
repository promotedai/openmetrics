package ai.promoted.metrics.logprocessor.job.contentetl;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkTableJob;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.paimon.PaimonSegment;
import ai.promoted.metrics.logprocessor.common.util.TableUtil;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(
    name = "contentetl",
    mixinStandardHelpOptions = true,
    version = "contentetl 1.0.0",
    description =
        "Creates a Flink batch job that read content JSON files and write them to Paimon tables.")
public class ContentEtlJob extends BaseFlinkTableJob {
  private static final Logger LOGGER = LogManager.getLogger(ContentEtlJob.class);
  @CommandLine.Mixin public final PaimonSegment paimonSegment = new PaimonSegment(this);

  @Option(
      names = {"--sourcePath"},
      defaultValue = "",
      description = "Source path for the content items.")
  public String contentSourcePath = "";

  @CommandLine.Option(
      names = {"--paimonTablePrefix"},
      defaultValue = "",
      description = "The prefix of paimon table name.")
  public String paimonTablePrefix = "";

  @Option(
      names = {"--contentType"},
      type = ContentType.class,
      description =
          "The type of the content items. Should match the s3 source path. Allowed values are contents and users. Defaults to contents.")
  public ContentType contentType = ContentType.contents;

  @Option(
      names = {"--sourceType"},
      type = SourceType.class,
      description =
          "Source Type for the content items. Allowed values are json and parquet. Defaults to json.")
  public SourceType sourceType = SourceType.json;

  @Option(
      names = {"--writeMode"},
      type = WriteMode.class,
      description = "Write mode. Can be UPSERT or OVERWRITE. Defaults to UPSERT.")
  public WriteMode writeMode = WriteMode.UPSERT;

  public enum WriteMode {
    UPSERT,
    OVERWRITE
  }

  public static void main(String[] args) {
    executeMain(new ContentEtlJob(), args);
  }

  @Override
  protected String getDefaultBaseJobName() {
    return "content-etl";
  }

  @Override
  public Set<FlinkSegment> getInnerFlinkSegments() {
    return ImmutableSet.of();
  }

  @Override
  public void tableOperationsToDataStream() {}

  @Override
  public void validateArgs() {
    super.validateArgs();
    Preconditions.checkArgument(!contentSourcePath.isBlank(), "basePath must be specified.");
  }

  @Override
  protected void startJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureExecutionEnvironment(env, parallelism, maxParallelism);
    env.setRestartStrategy(
        RestartStrategies.fixedDelayRestart(
            30, // Arbitrary to retry on failure.
            Time.of(30, TimeUnit.SECONDS)));
    TableResult tableResult = contentEtl(env);
    LOGGER.info("Job submitted. Waiting for execution result...");
    tableResult.await();
    LOGGER.info("Job finished.");
  }

  private String genCreateSourceTableSql() {
    switch (sourceType) {
      case json:
        switch (contentType) {
          case contents:
            return "CREATE TABLE "
                + "content_item (\n"
                + "  content_id STRING,\n"
                + "  properties STRING,\n"
                + "  last_updated BIGINT\n"
                + ") WITH (\n"
                + " 'connector' = 'filesystem',\n"
                + String.format(" 'path' = '%s',\n", contentSourcePath)
                + " 'json.fail-on-missing-field' = 'true',\n"
                + " 'format' = 'json'\n"
                + ")";
          case users:
            return "CREATE TABLE "
                + "content_item (\n"
                + "  user_id STRING,\n"
                + "  properties STRING,\n"
                + "  last_updated BIGINT\n"
                + ") WITH (\n"
                + " 'connector' = 'filesystem',\n"
                + String.format(" 'path' = '%s',\n", contentSourcePath)
                + " 'json.fail-on-missing-field' = 'true',\n"
                + " 'format' = 'json'\n"
                + ")";
          default:
            throw new IllegalArgumentException("Unknown content type " + contentType);
        }
      case parquet:
        switch (contentType) {
          case contents:
            return "CREATE TABLE "
                + "content_item (\n"
                + "  _id STRING,\n"
                + "  _doc STRING\n"
                + ") WITH (\n"
                + " 'connector' = 'filesystem',\n"
                + String.format(" 'path' = '%s',\n", contentSourcePath)
                + " 'format' = 'parquet'\n"
                + ")";
          case users:
            return "CREATE TABLE "
                + "content_item (\n"
                + "  _id STRING,\n"
                + "  _doc STRING\n"
                + ") WITH (\n"
                + " 'connector' = 'filesystem',\n"
                + String.format(" 'path' = '%s',\n", contentSourcePath)
                + " 'format' = 'parquet'\n"
                + ")";
          default:
            throw new IllegalArgumentException("Unknown content type " + contentType);
        }
      default:
        throw new IllegalArgumentException("Unknown source type " + contentType);
    }
  }

  private String genCreatePaimonTableSql(Map<String, String> extraOptions) {
    switch (contentType) {
      case contents:
        return "CREATE TABLE IF NOT EXISTS "
            + fullTableName()
            + "(\n"
            + "  content_id VARCHAR,\n"
            + "  last_updated BIGINT,\n"
            + "  properties VARCHAR,\n"
            + "  `dt` VARCHAR(20),\n"
            + "  `hr` VARCHAR(20))\n"
            + "WITH (\n"
            + paimonSegment.genTableOptionStr(
                List.of("content_id", "last_updated", "dt", "hr"),
                List.of("dt", "hr"),
                extraOptions)
            + ");";
      case users:
        return "CREATE TABLE IF NOT EXISTS "
            + fullTableName()
            + "(\n"
            + "  user_id VARCHAR,\n"
            + "  last_updated BIGINT,\n"
            + "  properties VARCHAR,\n"
            + "  `dt` VARCHAR(20),\n"
            + "  `hr` VARCHAR(20))\n"
            + "WITH (\n"
            + paimonSegment.genTableOptionStr(
                List.of("user_id", "last_updated", "dt", "hr"), List.of("dt", "hr"), extraOptions)
            + ");";
      default:
        throw new IllegalArgumentException("Unknown content type " + contentType);
    }
  }

  private String genInsertIntoPaimonTableSql(Map<String, String> extraOptions) {
    String writeString;
    switch (writeMode) {
      case UPSERT:
        writeString = "INTO";
        break;
      case OVERWRITE:
        writeString = "OVERWRITE";
        break;
      default:
        throw new IllegalArgumentException("Unknown writeMode " + writeMode);
    }
    switch (sourceType) {
      case json:
        switch (contentType) {
          case contents:
            return String.format(
                    "INSERT %s %s/*+ OPTIONS(%s) */",
                    writeString, fullTableName(), TableUtil.genTableOptionsStr(extraOptions))
                + " SELECT content_id, last_updated, properties,"
                + " DATE_FORMAT(TO_TIMESTAMP_LTZ(last_updated, 3), 'yyyy-MM-dd') dt,"
                + " DATE_FORMAT(TO_TIMESTAMP_LTZ(last_updated, 3), 'HH') hr"
                + " FROM content_item";
          case users:
            return String.format(
                    "INSERT %s %s/*+ OPTIONS(%s) */",
                    writeString, fullTableName(), TableUtil.genTableOptionsStr(extraOptions))
                + " SELECT user_id, last_updated, properties,"
                + " DATE_FORMAT(TO_TIMESTAMP_LTZ(last_updated, 3), 'yyyy-MM-dd') dt,"
                + " DATE_FORMAT(TO_TIMESTAMP_LTZ(last_updated, 3), 'HH') hr"
                + " FROM content_item";
          default:
            throw new IllegalArgumentException("Unknown content type " + contentType);
        }
      case parquet:
        switch (contentType) {
          case contents:
            return String.format(
                    "INSERT %s %s/*+ OPTIONS(%s) */",
                    writeString, fullTableName(), TableUtil.genTableOptionsStr(extraOptions))
                + " SELECT"
                + " _id as content_id,"
                + " CAST(JSON_VALUE(_doc, '$.last_updated.$date') AS BIGINT) as last_updated,"
                + " JSON_QUERY(_doc, '$.properties') as properties,"
                + " DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(_doc, '$.last_updated.$date') AS BIGINT), 3), 'yyyy-MM-dd') dt,"
                + " DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(_doc, '$.last_updated.$date') AS BIGINT), 3), 'HH') hr"
                + " FROM content_item";
          case users:
            return String.format(
                    "INSERT %s %s/*+ OPTIONS(%s) */",
                    writeString, fullTableName(), TableUtil.genTableOptionsStr(extraOptions))
                + " SELECT"
                + " _id as user_id,"
                + " CAST(JSON_VALUE(_doc, '$.last_updated.$date') AS BIGINT) as last_updated,"
                + " JSON_QUERY(_doc, '$.properties') as properties,"
                + " DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(_doc, '$.last_updated.$date') AS BIGINT), 3), 'yyyy-MM-dd') dt,"
                + " DATE_FORMAT(TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(_doc, '$.last_updated.$date') AS BIGINT), 3), 'HH') hr"
                + " FROM content_item";
          default:
            throw new IllegalArgumentException("Unknown content type " + contentType);
        }
      default:
        throw new IllegalArgumentException("Unknown source type " + sourceType);
    }
  }

  TableResult contentEtl(StreamExecutionEnvironment env) throws Exception {
    // Use batch mode to write CoW table
    Configuration configuration = new Configuration();
    EnvironmentSettings tEnvSettings =
        new EnvironmentSettings.Builder().inBatchMode().withConfiguration(configuration).build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, tEnvSettings);
    paimonSegment.setStreamTableEnvironment(tEnv);
    paimonSegment.createPaimonCatalog();
    String labeledDatabase = TableUtil.getLabeledDatabaseName(databaseName, getJobLabel());
    paimonSegment.createPaimonDatabase(labeledDatabase);
    Map<String, String> extraOptions =
        paimonSegment.resolveExtraOptions(databaseName, getTableName(), Collections.emptyMap());
    String createSourceTableSql = genCreateSourceTableSql();
    String createPaimonTableSql = genCreatePaimonTableSql(extraOptions);
    String insertIntoPaimonSql = genInsertIntoPaimonTableSql(extraOptions);

    LOGGER.info(createSourceTableSql);
    tEnv.executeSql(createSourceTableSql);
    LOGGER.info(createPaimonTableSql);
    tEnv.executeSql(createPaimonTableSql);
    LOGGER.info(insertIntoPaimonSql);
    return tEnv.executeSql(insertIntoPaimonSql);
  }

  private String getTableName() {
    return String.format("%s_%s", paimonTablePrefix, contentType);
  }

  public String fullTableName() {
    return String.format(
        "%s.%s.%s",
        paimonSegment.paimonCatalogName,
        TableUtil.getLabeledDatabaseName(databaseName, getJobLabel()),
        getTableName());
  }

  public enum ContentType {
    contents,
    users
  }

  public enum SourceType {
    json,
    parquet
  }
}
