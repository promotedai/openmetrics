package ai.promoted.metrics.logprocessor.job.usersequence;

import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink.RedisSinkCommand;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisStandaloneSink;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.DirectFlatOutputKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSourceSegment;
import ai.promoted.metrics.logprocessor.common.table.FlinkTableUtils;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.AttributedAction;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Set;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(
    name = "usersequence",
    mixinStandardHelpOptions = true,
    version = "usersequence alpha",
    description =
        "Creates a Flink job that reads joined events from kafka, keeps the latest N user sequence,"
            + "and outputs changelog results to Redis.")
public class UserSequenceJob extends BaseFlinkJob {
  private static final Logger LOGGER = LogManager.getLogger(UserSequenceJob.class);
  @CommandLine.Mixin public final KafkaSegment kafkaSegment = new KafkaSegment();

  @CommandLine.Mixin
  public final KafkaSourceSegment kafkaSourceSegment = new KafkaSourceSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final FlatOutputKafkaSegment flatOutputKafkaSegment =
      new FlatOutputKafkaSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final DirectFlatOutputKafkaSource directFlatOutputKafkaSource =
      new DirectFlatOutputKafkaSource(kafkaSourceSegment, flatOutputKafkaSegment);

  @Option(
      names = {"--sequenceSize"},
      defaultValue = "50",
      description = "The sequence size to keep per user per action class.   Default=50")
  Integer sequenceSize = 50;

  @Option(
      names = {"--shardedEndpoints"},
      defaultValue = "",
      description =
          "Endpoint URIs for sharded counter 'service' (e.g. redis://host1:6399,host2:6399,...).  "
              + "Default=''")
  String shardedEndpoints = "";

  @Option(
      names = {"--unshardedEndpoint", "--counterService"},
      defaultValue = "",
      description =
          "Endpoint URI for non-sharded counter 'service' (e.g. redis://localhost:6399/0).  Default=''")
  String unshardedEndpoint = "";

  public static void main(String[] args) {
    new CommandLine(new UserSequenceJob()).execute(args);
  }

  @Override
  protected void startJob() throws Exception {
    Configuration configuration = new Configuration();
    EnvironmentSettings tEnvSettings =
        new EnvironmentSettings.Builder().withConfiguration(configuration).build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureExecutionEnvironment(env, parallelism, maxParallelism);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, tEnvSettings);
    startSqlJob(env, tEnv);
  }

  @Override
  public void validateArgs() {
    super.validateArgs();
    Preconditions.checkArgument(sequenceSize > 0, "--sequenceSize needs to be greater than 0");
  }

  @Override
  protected String getDefaultBaseJobName() {
    return "user-sequence";
  }

  @Override
  public Set<FlinkSegment> getInnerFlinkSegments() {
    return ImmutableSet.of(
        kafkaSegment, kafkaSourceSegment, flatOutputKafkaSegment, directFlatOutputKafkaSource);
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.of();
  }

  public JobExecutionResult startSqlJob(StreamExecutionEnvironment env, StreamTableEnvironment tEnv)
      throws Exception {
    RedisSink redisSink = newRedisSink();
    return startUserSequenceJob(
        env,
        tEnv,
        directFlatOutputKafkaSource.getAttributedActionSource(
            env, toKafkaConsumerGroupId("user-sequence-m")),
        redisSink);
  }

  private RedisSink newRedisSink() {
    return StringUtil.isBlank(shardedEndpoints)
        ? new RedisStandaloneSink(unshardedEndpoint, ImmutableList.of())
        : new RedisSink(shardedEndpoints, ImmutableList.of());
  }

  public JobExecutionResult startUserSequenceJob(
      StreamExecutionEnvironment env,
      StreamTableEnvironment tEnv,
      SingleOutputStreamOperator<AttributedAction> attributedActions,
      SinkFunction<RedisSinkCommand> sinkFunction)
      throws Exception {
    this.disableAutoGeneratedUIDs = false; // required by Flink SQL
    FlinkTableUtils.registerProtoView(tEnv, attributedActions, "attributed_action");
    String query =
        "SELECT action.user_info.log_user_id, IF(action_type = 'NAVIGATE', 'N', 'O') as action_class, action.action_type, touchpoint.joined_impression.response_insertion.content_id, action.timing.event_api_timestamp\n"
            + "FROM "
            + "(SELECT *, ROW_NUMBER() OVER (PARTITION BY action.user_info.log_user_id, action.action_type = 'NAVIGATE' ORDER BY action.timing.event_api_timestamp DESC) AS row_num FROM attributed_action"
            + "   WHERE action_type <> '"
            + ActionType.UNKNOWN_ACTION_TYPE
            + "' AND action_type <> '"
            + ActionType.CUSTOM_ACTION_TYPE
            + "' AND attribution.model_id = 1"
            + ")\n"
            + "WHERE row_num <= "
            + sequenceSize;
    LOGGER.info(query);
    DataStream<Row> changelogStream = tEnv.toChangelogStream(tEnv.sqlQuery(query));
    changelogStream
        .map(
            new RichMapFunction<Row, RedisSinkCommand>() {
              @Override
              public RedisSinkCommand map(Row row) throws Exception {
                // field 0 -> log_user_id
                // field 1 -> action class (is NAVIGATE or not)
                // field 2 -> action_type
                // field 3 -> content_id
                // field 4 -> event_api_timestamp
                String key = String.join(RedisSink.JOIN_CHAR, row.getFieldAs(0), row.getFieldAs(1));
                String value =
                    String.join(
                        RedisSink.JOIN_CHAR,
                        row.getFieldAs(2),
                        row.getFieldAs(3),
                        String.valueOf(row.getField(4)));
                long score = row.getFieldAs(4);

                if (row.getKind() == RowKind.INSERT) {
                  return RedisSinkCommand.zadd(key, value, score);
                } else if (row.getKind() == RowKind.DELETE) {
                  return RedisSinkCommand.zrem(key, value);
                } else {
                  throw new RuntimeException("Unknown rowkind " + row);
                }
              }
            })
        .addSink(sinkFunction);
    return env.execute(getJobName());
  }
}
