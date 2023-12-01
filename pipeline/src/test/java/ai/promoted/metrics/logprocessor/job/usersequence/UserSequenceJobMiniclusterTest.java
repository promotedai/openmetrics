package ai.promoted.metrics.logprocessor.job.usersequence;

import static ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink.JOIN_CHAR;
import static com.google.common.truth.Truth.assertThat;

import ai.promoted.metrics.logprocessor.common.functions.inferred.AttributionModel;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink.RedisSinkCommand;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisStandaloneSink;
import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobMiniclusterTest;
import ai.promoted.metrics.logprocessor.common.testing.MiniClusterExtension;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.Attribution;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.Touchpoint;
import com.google.common.collect.ImmutableList;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@ExtendWith(MiniClusterExtension.class)
public class UserSequenceJobMiniclusterTest extends BaseJobMiniclusterTest<UserSequenceJob> {
  private static final Logger LOGGER = LogManager.getLogger(UserSequenceJobMiniclusterTest.class);
  private static final String USER_1 = "user_1";
  private static final String USER_2 = "user_2";

  // TODO Xingcan create a common joined event mock input
  private static AttributedAction genAttributedAction(
      String logUserId, ActionType actionType, long eventTime) {

    Action.Builder actionBuilder =
        Action.newBuilder()
            .setPlatformId(1)
            .setActionType(actionType)
            .setContentId("content_id")
            .setUserInfo(UserInfo.newBuilder().setLogUserId(logUserId));
    actionBuilder.getTimingBuilder().setEventApiTimestamp(eventTime);

    return AttributedAction.newBuilder()
        .setAction(actionBuilder)
        .setAttribution(
            Attribution.newBuilder().setModelId(AttributionModel.LATEST.id).setCreditMillis(1000))
        .setTouchpoint(
            Touchpoint.newBuilder()
                .setJoinedImpression(
                    JoinedImpression.newBuilder()
                        .setResponseInsertion(Insertion.newBuilder().setContentId("content_id"))))
        .build();
  }

  @Test
  void testRedisWriting() throws Exception {
    try (GenericContainer<?> redis =
        new GenericContainer<>(DockerImageName.parse(REDIS_CONTAINER_IMAGE))) {
      redis.withExposedPorts(6379);
      redis.start();
      String redisUri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);

      UserSequenceJob job = createJob();
      job.unshardedEndpoint = redisUri;
      //      job.shardedEndpoints = redisUri;
      job.sequenceSize = 2;
      RedisStandaloneSink redisSink = new RedisStandaloneSink(redisUri, Collections.emptyList());
      Configuration configuration = new Configuration();
      EnvironmentSettings tEnvSettings =
          new EnvironmentSettings.Builder().withConfiguration(configuration).build();
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, tEnvSettings);
      JobExecutionResult result =
          job.startUserSequenceJob(env, tEnv, createMockInput(env), redisSink);
      waitForDone(result);
      try (RedisClient clusterClient = RedisClient.create(redisUri)) {
        try (StatefulRedisConnection<String, String> connection = clusterClient.connect()) {
          // validate the keys
          List<String> keys = connection.sync().keys("*");
          List<String> expectedKeys =
              ImmutableList.of(
                  String.join(JOIN_CHAR, USER_1, "N"),
                  String.join(JOIN_CHAR, USER_1, "O"),
                  String.join(JOIN_CHAR, USER_2, "N"),
                  String.join(JOIN_CHAR, USER_2, "O"));
          assertThat(keys).containsExactlyElementsIn(expectedKeys);
          // validate user_1 navigate sequence
          List<String> user1Navigate =
              connection.sync().zrange(String.join(JOIN_CHAR, USER_1, "N"), 0L, -1L);
          List<String> user1NavigateExpected =
              ImmutableList.of(
                  String.join(JOIN_CHAR, ActionType.NAVIGATE.toString(), "content_id", "3000"),
                  String.join(JOIN_CHAR, ActionType.NAVIGATE.toString(), "content_id", "6000"));
          assertThat(user1Navigate).containsExactlyElementsIn(user1NavigateExpected);

          // validate user_1 other sequence
          List<String> user1Other =
              connection.sync().zrange(String.join(JOIN_CHAR, USER_1, "O"), 0L, -1L);
          List<String> user1OtherExpected =
              ImmutableList.of(
                  String.join(JOIN_CHAR, ActionType.CHECKOUT.toString(), "content_id", "9000"),
                  String.join(JOIN_CHAR, ActionType.CHECKOUT.toString(), "content_id", "10000"));
          assertThat(user1Other).containsExactlyElementsIn(user1OtherExpected);

          // validate user_2 navigate sequence
          List<String> user2Navigate =
              connection.sync().zrange(String.join(JOIN_CHAR, USER_2, "N"), 0L, -1L);
          List<String> user2NavigateExpected =
              ImmutableList.of(
                  String.join(JOIN_CHAR, ActionType.NAVIGATE.toString(), "content_id", "3500"),
                  String.join(JOIN_CHAR, ActionType.NAVIGATE.toString(), "content_id", "4500"));
          assertThat(user2Navigate).containsExactlyElementsIn(user2NavigateExpected);

          // validate user_2 other sequence
          List<String> user2Other =
              connection.sync().zrange(String.join(JOIN_CHAR, USER_2, "O"), 0L, -1L);
          List<String> user2OtherExpected =
              ImmutableList.of(
                  String.join(JOIN_CHAR, ActionType.ADD_TO_CART.toString(), "content_id", "5500"));
          assertThat(user2Other).containsExactlyElementsIn(user2OtherExpected);
        }
      }
    }
  }

  @Test
  void testGenRedisCommands() throws Exception {
    UserSequenceJob job = createJob();
    job.sequenceSize = 3;
    Configuration configuration = new Configuration();
    EnvironmentSettings tEnvSettings =
        new EnvironmentSettings.Builder().withConfiguration(configuration).build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, tEnvSettings);
    SingleOutputStreamOperator<AttributedAction> mockSource = createMockInput(env);
    CollectingSink<RedisSinkCommand> collectingSink = new CollectingSink<>();
    JobExecutionResult result = job.startUserSequenceJob(env, tEnv, mockSource, collectingSink);
    waitForDone(result);
    // Validate navigates (N) for user_1
    assertThat(CollectingSink.output)
        .containsAtLeast(
            genZadd(USER_1, ActionType.NAVIGATE, 1000),
            genZadd(USER_1, ActionType.NAVIGATE, 2000),
            genZadd(USER_1, ActionType.NAVIGATE, 3000),
            genZrem(USER_1, ActionType.NAVIGATE, 1000),
            genZadd(USER_1, ActionType.NAVIGATE, 6000))
        .inOrder();
    // Validate others (O) for user_1
    assertThat(CollectingSink.output)
        .containsAtLeast(
            genZadd(USER_1, ActionType.ADD_TO_CART, 4000),
            genZadd(USER_1, ActionType.PURCHASE, 8000),
            genZadd(USER_1, ActionType.CHECKOUT, 9000),
            genZrem(USER_1, ActionType.ADD_TO_CART, 4000),
            genZadd(USER_1, ActionType.CHECKOUT, 10000))
        .inOrder();
    // Validate navigate (N) for user_2
    assertThat(CollectingSink.output)
        .containsAtLeast(
            genZadd(USER_2, ActionType.NAVIGATE, 1500),
            genZadd(USER_2, ActionType.NAVIGATE, 2500),
            genZadd(USER_2, ActionType.NAVIGATE, 3500),
            genZrem(USER_2, ActionType.NAVIGATE, 1500),
            genZadd(USER_2, ActionType.NAVIGATE, 4500))
        .inOrder();
    // Validate others (O) for user_2
    assertThat(CollectingSink.output).contains(genZadd(USER_2, ActionType.ADD_TO_CART, 5500));
  }

  @BeforeEach
  void clearResults() {
    CollectingSink.output.clear();
  }

  private RedisSinkCommand genZadd(String user, ActionType actionType, long time) {
    String actionClass = actionType == ActionType.NAVIGATE ? "N" : "O";

    return RedisSinkCommand.zadd(
        String.join(JOIN_CHAR, user, actionClass),
        String.join(JOIN_CHAR, actionType.toString(), "content_id", String.valueOf(time)),
        time);
  }

  private RedisSinkCommand genZrem(String user, ActionType actionType, long time) {
    String actionClass = actionType == ActionType.NAVIGATE ? "N" : "O";
    return RedisSinkCommand.zrem(
        String.join(JOIN_CHAR, user, actionClass),
        String.join(JOIN_CHAR, actionType.toString(), "content_id", String.valueOf(time)));
  }

  private SingleOutputStreamOperator<AttributedAction> createMockInput(
      StreamExecutionEnvironment env) {
    List<AttributedAction> sourceList =
        List.of(
            genAttributedAction(USER_1, ActionType.NAVIGATE, 1000),
            genAttributedAction(USER_1, ActionType.NAVIGATE, 2000),
            genAttributedAction(USER_1, ActionType.NAVIGATE, 3000),
            genAttributedAction(USER_1, ActionType.ADD_TO_CART, 4000),
            genAttributedAction(USER_1, ActionType.CUSTOM_ACTION_TYPE, 5000),
            genAttributedAction(USER_1, ActionType.NAVIGATE, 6000),
            genAttributedAction(USER_1, ActionType.UNKNOWN_ACTION_TYPE, 7000),
            genAttributedAction(USER_1, ActionType.PURCHASE, 8000),
            genAttributedAction(USER_1, ActionType.CHECKOUT, 9000),
            genAttributedAction(USER_1, ActionType.CHECKOUT, 10000),
            genAttributedAction(USER_2, ActionType.NAVIGATE, 1500),
            genAttributedAction(USER_2, ActionType.NAVIGATE, 2500),
            genAttributedAction(USER_2, ActionType.NAVIGATE, 3500),
            genAttributedAction(USER_2, ActionType.NAVIGATE, 4500),
            genAttributedAction(USER_2, ActionType.ADD_TO_CART, 5500));

    // Non-last touch attributions get excluded.
    sourceList = addEvenAttributedActions(sourceList);

    ArrayList<AttributedAction> sortedList = new ArrayList<>(sourceList);
    sortedList.sort(Comparator.comparing(e -> e.getAction().getTiming().getEventApiTimestamp()));
    WatermarkStrategy<AttributedAction> watermarkStrategy =
        WatermarkStrategy.<AttributedAction>forMonotonousTimestamps()
            .withTimestampAssigner(
                (event, timestamp) -> event.getAction().getTiming().getEventApiTimestamp());
    return env.fromCollection(sortedList).assignTimestampsAndWatermarks(watermarkStrategy);
  }

  private static List<AttributedAction> addEvenAttributedActions(
      List<AttributedAction> attributedActions) {
    return attributedActions.stream()
        .flatMap(
            attributedAction ->
                Stream.of(attributedAction, cloneAndSetAttributionModelId(attributedAction, 2L)))
        .collect(Collectors.toList());
  }

  private static AttributedAction cloneAndSetAttributionModelId(
      AttributedAction attributedAction, long attributionModelId) {
    AttributedAction.Builder builder = attributedAction.toBuilder();
    builder.getAttributionBuilder().setModelId(attributionModelId);
    return builder.build();
  }

  @Override
  protected UserSequenceJob createJob() {
    UserSequenceJob job = new UserSequenceJob();
    job.disableAutoGeneratedUIDs = false;
    job.checkpointInterval = Duration.ofSeconds(2);
    job.checkpointTimeout = Duration.ofSeconds(60);
    job.configureExecutionEnvironment(env, 2, 10);
    return job;
  }

  static class CollectingSink<T> implements SinkFunction<T> {

    public static final List<Object> output = Collections.synchronizedList(new ArrayList<>());
    public long currentTimestamp = Long.MIN_VALUE;

    public void invoke(T value, Context ctx) {
      if (ctx != null && ctx.timestamp() != null) {
        currentTimestamp = ctx.timestamp();
      }
      output.add(value);
    }
  }
}
