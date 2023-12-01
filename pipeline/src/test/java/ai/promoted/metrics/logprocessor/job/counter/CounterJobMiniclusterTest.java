package ai.promoted.metrics.logprocessor.job.counter;

import static ai.promoted.metrics.logprocessor.common.counter.Constants.QUERY_TYPE;
import static ai.promoted.metrics.logprocessor.common.counter.Constants.USER_TYPE;
import static ai.promoted.metrics.logprocessor.common.util.StringUtil.xxhash;
import static ai.promoted.metrics.logprocessor.common.util.StringUtil.xxhashHex;
import static ai.promoted.metrics.logprocessor.job.counter.CounterJob.CSV;
import static ai.promoted.proto.event.ActionType.NAVIGATE;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.logprocessor.common.counter.CounterKeys;
import ai.promoted.metrics.logprocessor.common.counter.CounterKeys.CountKey;
import ai.promoted.metrics.logprocessor.common.counter.FeatureId;
import ai.promoted.metrics.logprocessor.common.counter.PlatformContentDeviceEvent;
import ai.promoted.metrics.logprocessor.common.counter.PlatformContentQueryEvent;
import ai.promoted.metrics.logprocessor.common.counter.PlatformDeviceEvent;
import ai.promoted.metrics.logprocessor.common.counter.PlatformLogUserContentEvent;
import ai.promoted.metrics.logprocessor.common.counter.PlatformLogUserEvent;
import ai.promoted.metrics.logprocessor.common.counter.PlatformLogUserQueryEvent;
import ai.promoted.metrics.logprocessor.common.counter.PlatformQueryEvent;
import ai.promoted.metrics.logprocessor.common.counter.PlatformUserContentEvent;
import ai.promoted.metrics.logprocessor.common.counter.PlatformUserEvent;
import ai.promoted.metrics.logprocessor.common.counter.PlatformUserQueryEvent;
import ai.promoted.metrics.logprocessor.common.functions.LastTimeAndCount;
import ai.promoted.metrics.logprocessor.common.functions.SlidingDailyCounter;
import ai.promoted.metrics.logprocessor.common.functions.SlidingSixDayCounter;
import ai.promoted.metrics.logprocessor.common.functions.inferred.AttributionModel;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink.RedisSinkCommand;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisStandaloneSink;
import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobMiniclusterTest;
import ai.promoted.metrics.logprocessor.common.queryablestate.StreamGraphRewriter;
import ai.promoted.metrics.logprocessor.common.testing.MiniClusterExtension;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.internal.features.AggMetric;
import ai.promoted.proto.delivery.internal.features.CountFeatureMask;
import ai.promoted.proto.delivery.internal.features.CountType;
import ai.promoted.proto.delivery.internal.features.CountWindow;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.Attribution;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.Touchpoint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@ExtendWith(MiniClusterExtension.class)
// TODO Optimize the test cases to run only once and reuse results
public class CounterJobMiniclusterTest extends BaseJobMiniclusterTest<CounterJob> {

  private static final Logger LOGGER = LogManager.getLogger(CounterJobMiniclusterTest.class);
  private static final TypeInformation<JoinedImpression> joinedImpressionTypeInfo =
      TypeExtractor.getForClass(JoinedImpression.class);
  private static final TypeInformation<AttributedAction> attributedActionTypeInfo =
      TypeExtractor.getForClass(AttributedAction.class);
  private static final TypeInformation<LogUserUser> logUserUserEventTypeInfo =
      TypeExtractor.getForClass(LogUserUser.class);
  private static final CollectingSink COLLECTING_SINK = new CollectingSink();
  private static final long DUR_10_MIN = Duration.ofMinutes(10).toMillis();
  private static final long DUR_30_MIN = Duration.ofMinutes(30).toMillis();
  private static final long DUR_2_HR = Duration.ofHours(2).toMillis();
  private static final long DUR_3_DAY = Duration.ofDays(3).toMillis();
  private static final long DUR_10_DAY = Duration.ofDays(10).toMillis();
  private static final long DUR_100_DAY = Duration.ofDays(100).toMillis();
  // Query string hex mapping:
  // "" => ef46db3751d8e999
  private static final String UPPER_CASE_QUERY_ONE = "Query One"; // QUERY_ONE => 28841ff692f4061a
  private static final String UPPER_CASE_QUERY_TWO = "Query Two"; // QUERY_TWO => 41809fa5f00aefaa
  private static final String LOWER_CASE_QUERY_ONE = "query one"; // QUERY_ONE => 28841ff692f4061a
  private static final String LOWER_CASE_QUERY_TWO = "query two"; // QUERY_TWO => 41809fa5f00aefaa
  private static final TestUser PROMOTED =
      new TestUser(
          "promoted/980 CFNetwork/1240.0.4 Darwin/20.6.0",
          "promoted",
          "iOS",
          "nios",
          "p_uid",
          "p_log_uid");
  private static final TestUser OKHTTP =
      new TestUser("okhttp/3.14.9", "okhttp", "Other", "other", "okhttp_uid", "okhttp_log_uid");
  private static final long PLATFORM_ID = 20L;
  // ============ Global Event Devices ================
  private static final PlatformDeviceEvent OK_HTTP_GLOBAL_EVENT_DVC =
      new PlatformDeviceEvent(PLATFORM_ID, OKHTTP.DVC, AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformDeviceEvent PROMOTED_GLOBAL_EVENT_DVC =
      new PlatformDeviceEvent(PLATFORM_ID, PROMOTED.DVC, AggMetric.COUNT_NAVIGATE.name());
  // ============ Global Event Devices ================

  // ============ Event Devices ================
  private static final PlatformContentDeviceEvent OK_HTTP_CONTENT_EVENT_DVC_10_MIN =
      new PlatformContentDeviceEvent(
          PLATFORM_ID,
          genContentId(NAVIGATE, DUR_10_MIN),
          OKHTTP.DVC,
          AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformContentDeviceEvent OK_HTTP_CONTENT_EVENT_DVC_2_HR =
      new PlatformContentDeviceEvent(
          PLATFORM_ID,
          genContentId(NAVIGATE, DUR_2_HR),
          OKHTTP.DVC,
          AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformContentDeviceEvent PROMOTED_HTTP_CONTENT_EVENT_DVC_30_MIN =
      new PlatformContentDeviceEvent(
          PLATFORM_ID,
          genContentId(NAVIGATE, DUR_30_MIN),
          PROMOTED.DVC,
          AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformContentDeviceEvent PROMOTED_HTTP_CONTENT_EVENT_DVC_3_DAY =
      new PlatformContentDeviceEvent(
          PLATFORM_ID,
          genContentId(NAVIGATE, DUR_3_DAY),
          PROMOTED.DVC,
          AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformContentDeviceEvent PROMOTED_HTTP_CONTENT_EVENT_DVC_10_DAY =
      new PlatformContentDeviceEvent(
          PLATFORM_ID,
          genContentId(NAVIGATE, DUR_10_DAY),
          PROMOTED.DVC,
          AggMetric.COUNT_NAVIGATE.name());
  // ============ Event Devices ================

  // ============ Query Events ================
  private static final PlatformQueryEvent PLATFORM_QUERY_EVENT_ONE =
      new PlatformQueryEvent(
          PLATFORM_ID, xxhash(LOWER_CASE_QUERY_ONE), AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformQueryEvent PLATFORM_QUERY_EVENT_TWO =
      new PlatformQueryEvent(
          PLATFORM_ID, xxhash(LOWER_CASE_QUERY_TWO), AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformQueryEvent PLATFORM_QUERY_EVENT_EMPTY =
      new PlatformQueryEvent(PLATFORM_ID, xxhash(""), AggMetric.COUNT_NAVIGATE.name());
  // ============ Query Events ================

  // ============ Content Query Events ================
  private static final PlatformContentQueryEvent contentQueryEventTwo10Min =
      new PlatformContentQueryEvent(
          PLATFORM_ID,
          xxhash(LOWER_CASE_QUERY_TWO),
          genContentId(NAVIGATE, DUR_10_MIN),
          AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformContentQueryEvent contentQueryEventTwo30Min =
      new PlatformContentQueryEvent(
          PLATFORM_ID,
          xxhash(LOWER_CASE_QUERY_TWO),
          genContentId(NAVIGATE, DUR_30_MIN),
          AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformContentQueryEvent contentQueryEventOne2Hr =
      new PlatformContentQueryEvent(
          PLATFORM_ID,
          xxhash(LOWER_CASE_QUERY_ONE),
          genContentId(NAVIGATE, DUR_2_HR),
          AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformContentQueryEvent contentQueryEventEmpty3Day =
      new PlatformContentQueryEvent(
          PLATFORM_ID,
          xxhash(""),
          genContentId(NAVIGATE, DUR_3_DAY),
          AggMetric.COUNT_NAVIGATE.name());

  private static final PlatformContentQueryEvent contentQueryEventEmpty10Day =
      new PlatformContentQueryEvent(
          PLATFORM_ID,
          xxhash(""),
          genContentId(NAVIGATE, DUR_10_DAY),
          AggMetric.COUNT_NAVIGATE.name());
  // ============ Content Query Events ================

  // ============ User/LogUser Events ================
  private static final PlatformUserEvent okHttpUserEvent =
      new PlatformUserEvent(PLATFORM_ID, OKHTTP.UID, AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformLogUserEvent okHttpLogUserEvent =
      new PlatformLogUserEvent(PLATFORM_ID, OKHTTP.L_UID, AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformUserEvent promotedUserEvent =
      new PlatformUserEvent(PLATFORM_ID, PROMOTED.UID, AggMetric.COUNT_NAVIGATE.name());
  private static final PlatformLogUserEvent promotedLogUserEvent =
      new PlatformLogUserEvent(PLATFORM_ID, PROMOTED.L_UID, AggMetric.COUNT_NAVIGATE.name());

  // ============ User/LogUser Events ================

  private static long getExpiry(long fid) {
    CountWindow window =
        CountWindow.forNumber(Math.toIntExact(fid & CountFeatureMask.WINDOW.getNumber()));
    switch (window) {
      case DAY_30:
        return SlidingDailyCounter.EXPIRE_TTL_SECONDS;
      case DAY_90:
        return SlidingSixDayCounter.EXPIRE_TTL_SECONDS;
      default:
        return 0;
    }
  }

  private static long getExpiry(long fid, long count) {
    CountWindow window =
        CountWindow.forNumber(Math.toIntExact(fid & CountFeatureMask.WINDOW.getNumber()));
    switch (window) {
      case DAY_30:
        throw new IllegalArgumentException("use the getExpiry(fid) for regular counts");
      case DAY_90:
        return count == 0
            ? LastTimeAndCount.EXPIRE_TTL_SECONDS
            : Math.toIntExact(Duration.ofDays(90 + 3).toSeconds());
      default:
        return Math.toIntExact(Duration.ofDays(90 + 3).toSeconds());
    }
  }

  private static JoinedImpression genJoinedImpression(
      Impression impression,
      @Nullable String logUserId,
      @Nullable String userAgent,
      @Nullable String query) {
    JoinedImpression.Builder builder = JoinedImpression.newBuilder();
    builder.getIdsBuilder().setPlatformId(PLATFORM_ID);
    if (!StringUtils.isNullOrWhitespaceOnly(logUserId)) {
      builder.getIdsBuilder().setLogUserId(logUserId);
    }
    if (!StringUtils.isNullOrWhitespaceOnly(userAgent)) {
      builder.getRequestBuilder().getDeviceBuilder().getBrowserBuilder().setUserAgent(userAgent);
    }
    if (!StringUtils.isNullOrWhitespaceOnly(query)) {
      builder.getRequestBuilder().setSearchQuery(query);
    }
    FlatUtil.setFlatImpression(builder, impression, (error) -> {});
    return builder.build();
  }

  private static AttributedAction genAttributedAction(
      Action action, JoinedImpression joinedImpression) {

    // TODO - looks like tests don't handle the case where action.content_id !=
    // impression.content_id.
    // Just force the IDs to be the same for now.
    JoinedImpression.Builder joinedImpressionBuilder = joinedImpression.toBuilder();
    joinedImpressionBuilder.getResponseInsertionBuilder().setContentId(action.getContentId());

    return AttributedAction.newBuilder()
        .setAction(action)
        .setAttribution(
            Attribution.newBuilder().setModelId(AttributionModel.LATEST.id).setCreditMillis(1000))
        .setTouchpoint(Touchpoint.newBuilder().setJoinedImpression(joinedImpressionBuilder))
        .build();
  }

  private static Action genAction(
      ActionType actionType, @Nullable String logUserId, long eventTime) {
    Action.Builder builder =
        Action.newBuilder()
            .setPlatformId(PLATFORM_ID)
            .setActionType(actionType)
            .setContentId(genContentId(actionType, eventTime));
    builder.getTimingBuilder().setEventApiTimestamp(eventTime);
    if (logUserId != null) {
      builder.setUserInfo(UserInfo.newBuilder().setLogUserId(logUserId));
    }
    return builder.build();
  }

  private static Impression genImpression(long eventTime) {
    Impression.Builder builder =
        Impression.newBuilder().setPlatformId(PLATFORM_ID).setContentId(genContentId(eventTime));
    builder.getTimingBuilder().setEventApiTimestamp(eventTime);
    return builder.build();
  }

  private static String genContentId(ActionType actionType, long eventTime) {
    return String.format("%s @ %s", actionType, eventTime);
  }

  private static String genContentId(long eventTime) {
    return String.format("c%s", eventTime);
  }

  private static LogUserUser genLogUserUser(
      String userId, String logUserId, long timestamp, long platformId) {
    return LogUserUser.newBuilder()
        .setUserId(userId)
        .setLogUserId(logUserId)
        .setEventTimeMillis(timestamp)
        .setPlatformId(platformId)
        .build();
  }

  private static List<AttributedAction> addEvenAttributedActions(
      List<AttributedAction> attributedActions) {
    return attributedActions.stream()
        .flatMap(
            attributedAction ->
                Stream.of(
                    attributedAction,
                    cloneAndSetAttributionModelId(attributedAction, AttributionModel.EVEN.id)))
        .collect(Collectors.toList());
  }

  private static AttributedAction cloneAndSetAttributionModelId(
      AttributedAction attributedAction, long attributionModelId) {
    AttributedAction.Builder builder = attributedAction.toBuilder();
    builder.getAttributionBuilder().setModelId(attributionModelId);
    return builder.build();
  }

  List<JoinedImpression> inputJoinedImpressions;
  List<AttributedAction> inputAttributedActions;
  List<LogUserUser> inputLogUserUsers;

  @AfterEach
  public void clearSink() {
    CollectingSink.output.clear();
    inputJoinedImpressions = null;
    inputAttributedActions = null;
    inputLogUserUsers = null;
  }

  @Override
  protected CounterJob createJob() {
    CounterJob job = new CounterJob();
    job.counterOutputStartTimestamp = 0;
    job.s3.rootPath = tempDir.getAbsolutePath();
    // Checkpoint more frequently so we don't hit part files.
    job.checkpointInterval = Duration.ofSeconds(1);
    job.checkpointTimeout = Duration.ofSeconds(10);
    job.configureExecutionEnvironment(env, 1, 0);
    job.counterResultsBuffer = Duration.ZERO;
    job.searchQueryWindowSize = Duration.ofDays(1);
    job.searchQueryWindowSlide = Duration.ofMinutes(1);
    job.searchQueryWindowMinThreshold = 2;
    return job;
  }

  private StreamGraph genCounterJobStreamGraph(CounterJob job) {
    return genCounterJobStreamGraph(job, false);
  }

  private StreamGraph genCounterJobStreamGraph(CounterJob job, boolean includeImpressions) {
    return genCounterJobStreamGraph(job, includeImpressions, COLLECTING_SINK);
  }

  private StreamGraph genCounterJobStreamGraph(
      CounterJob job, boolean includeImpressions, SinkFunction<RedisSinkCommand> sink) {
    setupCommonInputRecords(includeImpressions);
    return defineJob(job, sink);
  }

  private void setupCommonInputRecords(boolean includeImpressions) {
    // In reality, there is a difference between the user and non-user flat event streams, but it's
    // fine to reuse the stream for the purposes of testing.
    //
    ImmutableList.Builder<JoinedImpression> joinedImpressionListBuilder = ImmutableList.builder();
    if (includeImpressions) {
      joinedImpressionListBuilder.add(
          genJoinedImpression(
              genImpression(DUR_2_HR /*DUR_10_MIN*/),
              OKHTTP.L_UID,
              OKHTTP.RA,
              UPPER_CASE_QUERY_TWO));
    }
    ImmutableList.Builder<AttributedAction> attributedActionListBuilder = ImmutableList.builder();
    attributedActionListBuilder.add(
        genAttributedAction(
            genAction(NAVIGATE, OKHTTP.L_UID, DUR_10_MIN),
            genJoinedImpression(
                genImpression(DUR_10_MIN), OKHTTP.L_UID, OKHTTP.RA, UPPER_CASE_QUERY_TWO)),
        genAttributedAction(
            genAction(NAVIGATE, PROMOTED.L_UID, DUR_30_MIN),
            genJoinedImpression(
                genImpression(DUR_30_MIN), PROMOTED.L_UID, PROMOTED.RA, UPPER_CASE_QUERY_TWO)),
        genAttributedAction(
            genAction(NAVIGATE, OKHTTP.L_UID, DUR_2_HR),
            genJoinedImpression(
                genImpression(DUR_2_HR), OKHTTP.L_UID, OKHTTP.RA, UPPER_CASE_QUERY_ONE)),
        genAttributedAction(
            genAction(NAVIGATE, PROMOTED.L_UID, DUR_3_DAY),
            genJoinedImpression(genImpression(DUR_3_DAY), PROMOTED.L_UID, PROMOTED.RA, "")),
        genAttributedAction(
            genAction(NAVIGATE, PROMOTED.L_UID, DUR_10_DAY),
            genJoinedImpression(genImpression(DUR_10_DAY), PROMOTED.L_UID, PROMOTED.RA, "")));

    inputJoinedImpressions = joinedImpressionListBuilder.build();
    inputAttributedActions = attributedActionListBuilder.build();
    inputLogUserUsers =
        ImmutableList.of(
            // Add 10min delay to test for temporalJoinFirstDimensionDelay
            genLogUserUser(OKHTTP.UID, OKHTTP.L_UID, DUR_10_MIN + DUR_10_MIN, PLATFORM_ID),
            genLogUserUser(PROMOTED.UID, PROMOTED.L_UID, DUR_30_MIN, PLATFORM_ID),
            genLogUserUser(OKHTTP.UID, OKHTTP.L_UID, DUR_2_HR, PLATFORM_ID),
            genLogUserUser(PROMOTED.UID, PROMOTED.L_UID, DUR_3_DAY, PLATFORM_ID));
  }

  private StreamGraph defineJob(CounterJob job, SinkFunction<RedisSinkCommand> sink) {
    CollectionSource<JoinedImpression> joinedImpressionsInput =
        new CollectionSource<>(inputJoinedImpressions, e -> e.getTiming().getEventApiTimestamp());
    CollectionSource<AttributedAction> attributedActionsInput =
        new CollectionSource<>(
            inputAttributedActions, e -> e.getAction().getTiming().getEventApiTimestamp());

    CollectionSource<LogUserUser> logUserUserInput =
        new CollectionSource<>(inputLogUserUsers, LogUserUser::getEventTimeMillis);
    // TODO - something is probably off with the timing logic.  10m causes the user join to fail.
    job.temporalJoinFirstDimensionDelay = Duration.ofMinutes(30);
    return job.defineJob(
        fromCollectionSource(
            env,
            "joined-impressions",
            joinedImpressionsInput,
            joinedImpressionTypeInfo,
            JoinedImpression::getTiming),
        fromCollectionSource(
            env,
            "attributed-actions",
            attributedActionsInput,
            attributedActionTypeInfo,
            action -> action.getAction().getTiming()),
        fromCollectionSource(
            env,
            "log-user-user-events",
            logUserUserInput,
            logUserUserEventTypeInfo,
            e -> Timing.newBuilder().setEventApiTimestamp(e.getEventTimeMillis()).build()),
        sink);
  }

  @Test
  void testRedisWrite() throws Exception {
    try (GenericContainer<?> redis =
        new GenericContainer<>(DockerImageName.parse(REDIS_CONTAINER_IMAGE))) {
      redis.withExposedPorts(6379);
      redis.start();
      String redisUri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
      CounterJob job = createJob();
      job.count90d = true;
      genCounterJobStreamGraph(
          job, false, new RedisStandaloneSink(redisUri, job.createMetadataCommands()));
      waitForDone(env.execute("counter"));

      try (RedisClient clusterClient = RedisClient.create(redisUri)) {
        try (StatefulRedisConnection<String, String> connection = clusterClient.connect()) {
          List<String> keys = connection.sync().keys("*");
          assertThat(keys).containsExactly(CounterKeys.ROW_FORMAT_KEY, CounterKeys.FEATURE_IDS_KEY);
          Map<String, String> expectedRowFormats =
              job.getEnabledCountKeys().stream()
                  .collect(
                      Collectors.toMap(CountKey::getDeprecatedRedisName, CountKey::getRowFormat));
          Map<String, String> rowFormats = connection.sync().hgetall(CounterKeys.ROW_FORMAT_KEY);
          assertThat(rowFormats).isEqualTo(expectedRowFormats);

          Map<String, String> expectedFeatureIds =
              job.getEnabledCountKeys().stream()
                  .collect(
                      Collectors.toMap(
                          CountKey::getDeprecatedRedisName, v -> CSV.join(v.getFeatureIds())));
          Map<String, String> featureIds = connection.sync().hgetall(CounterKeys.FEATURE_IDS_KEY);
          assertThat(featureIds).isEqualTo(expectedFeatureIds);
        }
      }
    }
  }

  @Test
  void testCounterStreamGraphRewriting() throws Exception {
    CounterJob job = createJob();
    job.counterResultsBuffer = Duration.ofDays(14);
    job.enableQueryableState = true;
    setupCommonInputRecords(true);
    StreamGraph graph = defineJob(job, COLLECTING_SINK);
    // Check that the stream graph has been rewritten to include the state query operators.
    assertThat(
            StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(
                        graph.getJobGraph().getVertices().iterator(), Spliterator.ORDERED),
                    false)
                .noneMatch(
                    vertex ->
                        vertex.getName().startsWith(StreamGraphRewriter.QUERY_OPERATOR_UID_PREFIX)))
        .isFalse();
    waitForDone(env.execute(graph));
  }

  @Test
  void basicCounts() throws Exception {
    CounterJob job = createJob();
    job.countKeys = ImmutableSet.of(PlatformDeviceEvent.NAME, PlatformContentDeviceEvent.NAME);
    // Enable this in one test to make sure it works.
    job.s3FileOutput.sideOutputDebugLogging = true;
    job.count90d = true;

    waitForDone(env.execute(genCounterJobStreamGraph(job)));
    assertBasicCountsCase(job);
  }

  @Test
  void excludeNonLastTouchpointAttributionModels() throws Exception {
    CounterJob job = createJob();
    job.countKeys = ImmutableSet.of(PlatformDeviceEvent.NAME, PlatformContentDeviceEvent.NAME);
    // Enable this in one test to make sure it works.
    job.s3FileOutput.sideOutputDebugLogging = true;
    job.count90d = true;

    setupCommonInputRecords(false);
    inputAttributedActions = addEvenAttributedActions(inputAttributedActions);
    defineJob(job, COLLECTING_SINK);
    waitForDone(env.execute("counter"));

    // Same asserts since we exclude other attribution models.
    assertBasicCountsCase(job);
  }

  private void assertBasicCountsCase(CounterJob job) {
    // Rather than test all the redis commands, just check that certain values were set.
    // Globals - note we expect globals to never expire

    for (Tuple2<Tuple, Integer> redisFieldAndCnt :
        ImmutableList.of(
            Tuple2.of(OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashField(1, HOURS), 1),
            Tuple2.of(OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashField(1, DAYS), 2),
            Tuple2.of(OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashField(7, DAYS), 2),
            Tuple2.of(OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashField(30, DAYS), 2),
            Tuple2.of(OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashField(90, DAYS), 2))) {

      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashKey(),
                  redisFieldAndCnt.f0,
                  redisFieldAndCnt.f1,
                  -1),
              // We will always get a 0 result since watermark=Long.MAX_VALUE will purge everything.
              RedisSink.hdel(OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashKey(), redisFieldAndCnt.f0))
          .inOrder();

      assertThat(CollectingSink.output)
          .doesNotContain(
              RedisSink.hset(
                  OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashKey(),
                  redisFieldAndCnt.f0,
                  redisFieldAndCnt.f1 + 1,
                  -1));
    }

    for (Tuple2<Tuple, Integer> redisFieldAndCnt :
        ImmutableList.of(
            Tuple2.of(PROMOTED_GLOBAL_EVENT_DVC.toRedisHashField(1, HOURS), 1),
            Tuple2.of(PROMOTED_GLOBAL_EVENT_DVC.toRedisHashField(1, DAYS), 1),
            Tuple2.of(PROMOTED_GLOBAL_EVENT_DVC.toRedisHashField(7, DAYS), 2),
            Tuple2.of(PROMOTED_GLOBAL_EVENT_DVC.toRedisHashField(30, DAYS), 3),
            Tuple2.of(PROMOTED_GLOBAL_EVENT_DVC.toRedisHashField(90, DAYS), 3))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  PROMOTED_GLOBAL_EVENT_DVC.toRedisHashKey(),
                  redisFieldAndCnt.f0,
                  redisFieldAndCnt.f1,
                  -1),
              RedisSink.hdel(PROMOTED_GLOBAL_EVENT_DVC.toRedisHashKey(), redisFieldAndCnt.f0))
          .inOrder();

      assertThat(CollectingSink.output)
          .doesNotContain(
              RedisSink.hset(
                  PROMOTED_GLOBAL_EVENT_DVC.toRedisHashKey(),
                  redisFieldAndCnt.f0,
                  redisFieldAndCnt.f1 + 1,
                  -1));
    }

    // Item: everything should be unique since they are id'd by timestamp

    for (Tuple hashField :
        ImmutableList.of(
            OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashField(1, HOURS),
            OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashField(1, DAYS),
            OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashField(7, DAYS),
            OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashField(30, DAYS),
            OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashField(90, DAYS))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(1))),
              RedisSink.hset(
                  OK_HTTP_CONTENT_EVENT_DVC_2_HR.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(1))))
          .inOrder();
      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashKey(),
                  hashField,
                  2,
                  getExpiry(hashField.getField(1))),
              RedisSink.hset(
                  OK_HTTP_CONTENT_EVENT_DVC_2_HR.toRedisHashKey(),
                  hashField,
                  2,
                  getExpiry(hashField.getField(1))));
    }

    for (Tuple hashField :
        ImmutableList.of(
            PROMOTED_HTTP_CONTENT_EVENT_DVC_30_MIN.toRedisHashField(1, HOURS),
            PROMOTED_HTTP_CONTENT_EVENT_DVC_30_MIN.toRedisHashField(1, DAYS),
            PROMOTED_HTTP_CONTENT_EVENT_DVC_30_MIN.toRedisHashField(7, DAYS),
            PROMOTED_HTTP_CONTENT_EVENT_DVC_30_MIN.toRedisHashField(30, DAYS),
            PROMOTED_HTTP_CONTENT_EVENT_DVC_30_MIN.toRedisHashField(90, DAYS))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  PROMOTED_HTTP_CONTENT_EVENT_DVC_30_MIN.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(1))),
              RedisSink.hset(
                  PROMOTED_HTTP_CONTENT_EVENT_DVC_3_DAY.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(1))),
              RedisSink.hset(
                  PROMOTED_HTTP_CONTENT_EVENT_DVC_10_DAY.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(1))),
              RedisSink.hdel(PROMOTED_HTTP_CONTENT_EVENT_DVC_10_DAY.toRedisHashKey(), hashField))
          .inOrder();

      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  PROMOTED_HTTP_CONTENT_EVENT_DVC_30_MIN.toRedisHashKey(),
                  hashField,
                  2,
                  getExpiry(hashField.getField(1))),
              RedisSink.hset(
                  PROMOTED_HTTP_CONTENT_EVENT_DVC_3_DAY.toRedisHashKey(),
                  hashField,
                  2,
                  getExpiry(hashField.getField(1))),
              RedisSink.hset(
                  PROMOTED_HTTP_CONTENT_EVENT_DVC_10_DAY.toRedisHashKey(),
                  hashField,
                  2,
                  getExpiry(hashField.getField(1))));
    }
    // TODO enable this check when the results become deterministic
    //        assertThat(CollectingSink.output).hasSize(335);
  }

  @Test
  void basicCounts_buffered() throws Exception {
    CounterJob job = createJob();
    job.countKeys = ImmutableSet.of(PlatformDeviceEvent.NAME, PlatformContentDeviceEvent.NAME);

    // Because the entire input window is in the buffer, only 0 values should be output.
    job.counterResultsBuffer = Duration.ofDays(14);
    waitForDone(env.execute(genCounterJobStreamGraph(job)));

    // Rather than test all the redis commands, just check that certain values were set.
    // Globals - note we expect globals to never expire
    for (Tuple hashField :
        ImmutableList.of(
            OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashField(1, HOURS),
            OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashField(1, DAYS),
            OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashField(7, DAYS),
            OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashField(30, DAYS))) {
      assertThat(CollectingSink.output)
          .contains(RedisSink.hdel(OK_HTTP_GLOBAL_EVENT_DVC.toRedisHashKey(), hashField));

      assertThat(CollectingSink.output)
          .doesNotContain(RedisSink.hset(OK_HTTP_GLOBAL_EVENT_DVC, hashField, 1, -1));
    }
    for (Tuple hashField :
        ImmutableList.of(
            PROMOTED_GLOBAL_EVENT_DVC.toRedisHashField(1, HOURS),
            PROMOTED_GLOBAL_EVENT_DVC.toRedisHashField(1, DAYS),
            PROMOTED_GLOBAL_EVENT_DVC.toRedisHashField(7, DAYS),
            PROMOTED_GLOBAL_EVENT_DVC.toRedisHashField(30, DAYS))) {
      assertThat(CollectingSink.output)
          .contains(RedisSink.hdel(PROMOTED_GLOBAL_EVENT_DVC.toRedisHashKey(), hashField));

      assertThat(CollectingSink.output)
          .doesNotContain(
              RedisSink.hset(PROMOTED_GLOBAL_EVENT_DVC.toRedisHashKey(), hashField, 1, -1));
    }

    // Items
    for (Tuple hashField :
        ImmutableList.of(
            OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashField(1, HOURS),
            OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashField(1, DAYS),
            OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashField(7, DAYS),
            OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashField(30, DAYS))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hdel(OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashKey(), hashField),
              RedisSink.hdel(OK_HTTP_CONTENT_EVENT_DVC_2_HR.toRedisHashKey(), hashField));

      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  OK_HTTP_CONTENT_EVENT_DVC_10_MIN.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(1))),
              RedisSink.hset(
                  OK_HTTP_CONTENT_EVENT_DVC_2_HR.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(1))));
    }
    for (Tuple hashField :
        ImmutableList.of(
            PROMOTED_HTTP_CONTENT_EVENT_DVC_10_DAY.toRedisHashField(1, HOURS),
            PROMOTED_HTTP_CONTENT_EVENT_DVC_10_DAY.toRedisHashField(1, DAYS),
            PROMOTED_HTTP_CONTENT_EVENT_DVC_10_DAY.toRedisHashField(7, DAYS),
            PROMOTED_HTTP_CONTENT_EVENT_DVC_10_DAY.toRedisHashField(30, DAYS))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hdel(PROMOTED_HTTP_CONTENT_EVENT_DVC_30_MIN.toRedisHashKey(), hashField),
              RedisSink.hdel(PROMOTED_HTTP_CONTENT_EVENT_DVC_3_DAY.toRedisHashKey(), hashField),
              RedisSink.hdel(PROMOTED_HTTP_CONTENT_EVENT_DVC_10_DAY.toRedisHashKey(), hashField));

      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  PROMOTED_HTTP_CONTENT_EVENT_DVC_30_MIN.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(1))),
              RedisSink.hset(
                  PROMOTED_HTTP_CONTENT_EVENT_DVC_3_DAY.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(1))),
              RedisSink.hset(
                  PROMOTED_HTTP_CONTENT_EVENT_DVC_10_DAY.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(1))));
    }
  }

  @Test
  void queryCounts() throws Exception {
    CounterJob job = createJob();
    job.countKeys = ImmutableSet.of(PlatformQueryEvent.NAME, PlatformContentQueryEvent.NAME);

    waitForDone(env.execute(genCounterJobStreamGraph(job)));

    // Query hourly
    // TODO - fix this.  The changes to getTestWatermarkStrategy to output onEvent causes this to
    // fail.
    /*
    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                PLATFORM_QUERY_EVENT_TWO.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_TWO.toRedisHashField(1, HOURS),
                2,
                0),
            RedisSink.hdel(
                PLATFORM_QUERY_EVENT_TWO.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_TWO.toRedisHashField(1, HOURS)))
        .inOrder();

    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                PLATFORM_QUERY_EVENT_EMPTY.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_EMPTY.toRedisHashField(1, HOURS),
                1,
                0),
            RedisSink.hdel(
                PLATFORM_QUERY_EVENT_EMPTY.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_EMPTY.toRedisHashField(1, HOURS)))
        .inOrder();

    assertThat(CollectingSink.output)
        .containsNoneOf(
            RedisSink.hset(
                PLATFORM_QUERY_EVENT_ONE.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_ONE.toRedisHashField(1, HOURS),
                1,
                0),
            RedisSink.hdel(
                PLATFORM_QUERY_EVENT_ONE.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_ONE.toRedisHashField(1, HOURS)));
     */

    // Query daily
    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                PLATFORM_QUERY_EVENT_TWO.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_TWO.toRedisHashField(1, DAYS),
                2,
                0),
            RedisSink.hdel(
                PLATFORM_QUERY_EVENT_TWO.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_TWO.toRedisHashField(1, DAYS)))
        .inOrder();
    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                PLATFORM_QUERY_EVENT_TWO.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_TWO.toRedisHashField(1, DAYS),
                2,
                0),
            RedisSink.hdel(
                PLATFORM_QUERY_EVENT_TWO.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_TWO.toRedisHashField(1, DAYS)))
        .inOrder();
    assertThat(CollectingSink.output)
        .containsNoneOf(
            RedisSink.hset(
                PLATFORM_QUERY_EVENT_ONE.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_ONE.toRedisHashField(1, DAYS),
                1,
                0),
            RedisSink.hdel(
                PLATFORM_QUERY_EVENT_ONE.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_ONE.toRedisHashField(1, DAYS)));

    // Query 7d
    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                PLATFORM_QUERY_EVENT_TWO.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_TWO.toRedisHashField(7, DAYS),
                2,
                0),
            RedisSink.hdel(
                PLATFORM_QUERY_EVENT_TWO.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_TWO.toRedisHashField(7, DAYS)))
        .inOrder();
    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                PLATFORM_QUERY_EVENT_EMPTY.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_EMPTY.toRedisHashField(7, DAYS),
                1,
                0),
            RedisSink.hdel(
                PLATFORM_QUERY_EVENT_EMPTY.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_EMPTY.toRedisHashField(7, DAYS)))
        .inOrder();
    assertThat(CollectingSink.output)
        .containsNoneOf(
            RedisSink.hset(
                PLATFORM_QUERY_EVENT_ONE.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_ONE.toRedisHashField(7, DAYS),
                1,
                0),
            RedisSink.hdel(
                PLATFORM_QUERY_EVENT_ONE.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_ONE.toRedisHashField(7, DAYS)));

    // Query 30d needs to be broken up due to likely bug in truth's inOrder evaluation.
    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                PLATFORM_QUERY_EVENT_TWO.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_TWO.toRedisHashField(30, DAYS),
                2,
                getExpiry(PLATFORM_QUERY_EVENT_TWO.toRedisHashField(30, DAYS).getField(0))),
            RedisSink.hdel(
                PLATFORM_QUERY_EVENT_TWO.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_TWO.toRedisHashField(30, DAYS)))
        .inOrder();

    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                PLATFORM_QUERY_EVENT_EMPTY.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_EMPTY.toRedisHashField(30, DAYS),
                2,
                getExpiry(PLATFORM_QUERY_EVENT_EMPTY.toRedisHashField(30, DAYS).getField(0))),
            RedisSink.hdel(
                PLATFORM_QUERY_EVENT_EMPTY.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_EMPTY.toRedisHashField(30, DAYS)))
        .inOrder();
    assertThat(CollectingSink.output)
        .doesNotContain(
            RedisSink.hset(
                PLATFORM_QUERY_EVENT_ONE.toRedisHashKey(),
                PLATFORM_QUERY_EVENT_ONE.toRedisHashField(30, DAYS),
                1,
                getExpiry(PLATFORM_QUERY_EVENT_ONE.toRedisHashField(30, DAYS).getField(0))));

    // Item x Query
    for (Tuple hashField :
        ImmutableList.of(
            contentQueryEventTwo10Min.toRedisHashField(1, HOURS),
            contentQueryEventTwo10Min.toRedisHashField(1, DAYS),
            contentQueryEventTwo10Min.toRedisHashField(7, DAYS),
            contentQueryEventTwo10Min.toRedisHashField(30, DAYS))) {

      // TODO - fix this.  The changes to getTestWatermarkStrategy to output onEvent causes this to
      // fail.
      /*
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  contentQueryEventTwo10Min.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(0))),
              RedisSink.hset(
                  contentQueryEventTwo30Min.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(0))))
          .inOrder();
       */
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  contentQueryEventEmpty3Day.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(0))),
              RedisSink.hset(
                  contentQueryEventEmpty10Day.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(0))))
          .inOrder();
      assertThat(CollectingSink.output)
          .doesNotContain(
              RedisSink.hset(
                  contentQueryEventOne2Hr.toRedisHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(0))));
    }
  }

  @Test
  void userCounts() throws Exception {
    CounterJob job = createJob();
    job.countKeys = ImmutableSet.of(PlatformLogUserEvent.NAME, PlatformUserEvent.NAME);
    genCounterJobStreamGraph(job, true);
    waitForDone(env.execute("counter"));

    // User: todd
    for (Tuple3<Tuple, Tuple, Integer> test :
        ImmutableList.of(
            Tuple3.of(
                okHttpLogUserEvent.toRedisHashField(1, HOURS),
                okHttpUserEvent.toRedisHashField(1, HOURS),
                1),
            Tuple3.of(
                okHttpLogUserEvent.toRedisHashField(1, DAYS),
                okHttpUserEvent.toRedisHashField(1, DAYS),
                2),
            Tuple3.of(
                okHttpLogUserEvent.toRedisHashField(7, DAYS),
                okHttpUserEvent.toRedisHashField(7, DAYS),
                2),
            Tuple3.of(
                okHttpLogUserEvent.toRedisHashField(30, DAYS),
                okHttpUserEvent.toRedisHashField(30, DAYS),
                2))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  okHttpLogUserEvent.toRedisHashKey(),
                  test.f0,
                  test.f2,
                  getExpiry(test.f0.getField(0))),
              RedisSink.hdel(okHttpLogUserEvent.toRedisHashKey(), test.f0))
          .inOrder();
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  okHttpUserEvent.toRedisHashKey(),
                  test.f1,
                  test.f2,
                  getExpiry(test.f1.getField(0))),
              RedisSink.hdel(okHttpUserEvent.toRedisHashKey(), test.f1))
          .inOrder();

      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  okHttpLogUserEvent.toRedisHashKey(),
                  test.f0,
                  test.f2 + 1,
                  getExpiry(test.f0.getField(0))),
              RedisSink.hset(
                  okHttpUserEvent.toRedisHashKey(),
                  test.f1,
                  test.f2 + 1,
                  getExpiry(test.f1.getField(0))));
    }

    // User: eve
    for (Tuple3<Tuple, Tuple, Integer> test :
        ImmutableList.of(
            Tuple3.of(
                promotedLogUserEvent.toRedisHashField(1, HOURS),
                promotedUserEvent.toRedisHashField(1, HOURS),
                1),
            Tuple3.of(
                promotedLogUserEvent.toRedisHashField(1, DAYS),
                promotedUserEvent.toRedisHashField(1, DAYS),
                1),
            Tuple3.of(
                promotedLogUserEvent.toRedisHashField(7, DAYS),
                promotedUserEvent.toRedisHashField(7, DAYS),
                2),
            Tuple3.of(
                promotedLogUserEvent.toRedisHashField(30, DAYS),
                promotedUserEvent.toRedisHashField(30, DAYS),
                3))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  promotedLogUserEvent.toRedisHashKey(),
                  test.f0,
                  test.f2,
                  getExpiry(test.f0.getField(0))),
              RedisSink.hdel(promotedLogUserEvent.toRedisHashKey(), test.f0))
          .inOrder();
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  promotedUserEvent.toRedisHashKey(),
                  test.f1,
                  test.f2,
                  getExpiry(test.f1.getField(0))),
              RedisSink.hdel(promotedUserEvent.toRedisHashKey(), test.f1))
          .inOrder();

      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  promotedLogUserEvent.toRedisHashKey(),
                  test.f0,
                  test.f2 + 1,
                  getExpiry(test.f0.getField(0))),
              RedisSink.hset(
                  promotedUserEvent.toRedisHashKey(),
                  test.f1,
                  test.f2 + 1,
                  getExpiry(test.f1.getField(0))));
    }

    PlatformUserEvent userImpressionCount =
        new PlatformUserEvent(PLATFORM_ID, OKHTTP.UID, AggMetric.COUNT_IMPRESSION.name());

    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                userImpressionCount.toRedisHashKey(),
                userImpressionCount.toRedisHashField(7, DAYS),
                1,
                getExpiry(userImpressionCount.toRedisHashField(7, DAYS).getField(0))),
            RedisSink.hset(
                userImpressionCount.toRedisHashKey(),
                userImpressionCount.toRedisHashField(1, DAYS),
                1,
                getExpiry(userImpressionCount.toRedisHashField(1, DAYS).getField(0))),
            RedisSink.hset(
                userImpressionCount.toRedisHashKey(),
                userImpressionCount.toRedisHashField(30, DAYS),
                1,
                SlidingDailyCounter.EXPIRE_TTL_SECONDS));
  }

  @Test
  void logUserItemLastTime() throws Exception {
    CounterJob job = createJob();
    job.countKeys =
        ImmutableSet.of(PlatformLogUserContentEvent.NAME, PlatformUserContentEvent.NAME);
    waitForDone(env.execute(genCounterJobStreamGraph(job)));
    List<CountType> logUserItemCountTypes =
        ImmutableList.of(
            CountType.USER_ITEM_COUNT,
            CountType.USER_ITEM_HOURS_AGO,
            CountType.LOG_USER_ITEM_COUNT,
            CountType.LOG_USER_ITEM_HOURS_AGO);
    for (CountType countType : logUserItemCountTypes) {
      boolean isLogUser =
          EnumSet.of(CountType.LOG_USER_ITEM_COUNT, CountType.LOG_USER_ITEM_HOURS_AGO)
              .contains(countType);
      String okhttpUid = isLogUser ? OKHTTP.L_UID : OKHTTP.UID;
      String promotedUid = isLogUser ? PROMOTED.L_UID : PROMOTED.UID;

      boolean isTimestamp =
          EnumSet.of(CountType.USER_ITEM_HOURS_AGO, CountType.LOG_USER_ITEM_HOURS_AGO)
              .contains(countType);
      long fid =
          isTimestamp
              ? FeatureId.lastUserContentTimestamp(isLogUser, AggMetric.COUNT_NAVIGATE)
              : FeatureId.lastUserContentCount(isLogUser, AggMetric.COUNT_NAVIGATE);

      // Rather than test all the redis commands, just check that certain values were set.
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  Tuple4.of(PLATFORM_ID, USER_TYPE, okhttpUid, genContentId(NAVIGATE, DUR_10_MIN)),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_10_MIN : 1),
                  getExpiry(fid, 1)),
              RedisSink.hdel(
                  Tuple4.of(PLATFORM_ID, USER_TYPE, okhttpUid, genContentId(NAVIGATE, DUR_10_MIN)),
                  Tuple1.of(fid)))
          .inOrder();
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  Tuple4.of(PLATFORM_ID, USER_TYPE, okhttpUid, genContentId(NAVIGATE, DUR_2_HR)),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_2_HR : 1),
                  getExpiry(fid, 1)),
              RedisSink.hdel(
                  Tuple4.of(PLATFORM_ID, USER_TYPE, okhttpUid, genContentId(NAVIGATE, DUR_2_HR)),
                  Tuple1.of(fid)))
          .inOrder();
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  Tuple4.of(
                      PLATFORM_ID, USER_TYPE, promotedUid, genContentId(NAVIGATE, DUR_30_MIN)),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_30_MIN : 1),
                  getExpiry(fid, 1)),
              RedisSink.hset(
                  Tuple4.of(PLATFORM_ID, USER_TYPE, promotedUid, genContentId(NAVIGATE, DUR_3_DAY)),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_3_DAY : 1),
                  getExpiry(fid, 1)),
              RedisSink.hset(
                  Tuple4.of(
                      PLATFORM_ID, USER_TYPE, promotedUid, genContentId(NAVIGATE, DUR_10_DAY)),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_10_DAY : 1),
                  getExpiry(fid, 1)),
              RedisSink.hdel(
                  Tuple4.of(
                      PLATFORM_ID, USER_TYPE, promotedUid, genContentId(NAVIGATE, DUR_30_MIN)),
                  Tuple1.of(fid)),
              RedisSink.hdel(
                  Tuple4.of(PLATFORM_ID, USER_TYPE, promotedUid, genContentId(NAVIGATE, DUR_3_DAY)),
                  Tuple1.of(fid)),
              RedisSink.hdel(
                  Tuple4.of(
                      PLATFORM_ID, USER_TYPE, promotedUid, genContentId(NAVIGATE, DUR_10_DAY)),
                  Tuple1.of(fid)))
          .inOrder();
    }
  }

  @Test
  void logUserQueryLastTime() throws Exception {
    CounterJob job = createJob();
    job.countKeys = ImmutableSet.of(PlatformLogUserQueryEvent.NAME, PlatformUserQueryEvent.NAME);
    waitForDone(env.execute(genCounterJobStreamGraph(job)));
    List<CountType> countTypes =
        ImmutableList.of(
            CountType.USER_QUERY_COUNT,
            CountType.USER_QUERY_HOURS_AGO,
            CountType.LOG_USER_QUERY_COUNT,
            CountType.LOG_USER_QUERY_HOURS_AGO);

    for (CountType countType : countTypes) {
      boolean isLogUser =
          EnumSet.of(CountType.LOG_USER_ITEM_COUNT, CountType.LOG_USER_ITEM_HOURS_AGO)
              .contains(countType);
      String okhttpUid = isLogUser ? OKHTTP.L_UID : OKHTTP.UID;
      String promotedUid = isLogUser ? PROMOTED.L_UID : PROMOTED.UID;

      boolean isTimestamp =
          EnumSet.of(CountType.USER_ITEM_HOURS_AGO, CountType.LOG_USER_ITEM_HOURS_AGO)
              .contains(countType);
      long fid =
          isTimestamp
              ? FeatureId.lastUserQueryTimestamp(isLogUser, AggMetric.COUNT_NAVIGATE)
              : FeatureId.lastUserQueryCount(isLogUser, AggMetric.COUNT_NAVIGATE);

      // Rather than test all the redis commands, just check that certain values were set.
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  Tuple5.of(
                      PLATFORM_ID,
                      USER_TYPE,
                      okhttpUid,
                      QUERY_TYPE,
                      xxhashHex(LOWER_CASE_QUERY_TWO)),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_10_MIN : 1),
                  getExpiry(fid, 1)),
              RedisSink.hdel(
                  Tuple5.of(
                      PLATFORM_ID,
                      USER_TYPE,
                      okhttpUid,
                      QUERY_TYPE,
                      xxhashHex(LOWER_CASE_QUERY_TWO)),
                  Tuple1.of(fid)))
          .inOrder();
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  Tuple5.of(
                      PLATFORM_ID,
                      USER_TYPE,
                      promotedUid,
                      QUERY_TYPE,
                      xxhashHex(LOWER_CASE_QUERY_TWO)),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_30_MIN : 1),
                  getExpiry(fid, 1)),
              RedisSink.hdel(
                  Tuple5.of(
                      PLATFORM_ID,
                      USER_TYPE,
                      promotedUid,
                      QUERY_TYPE,
                      xxhashHex(LOWER_CASE_QUERY_TWO)),
                  Tuple1.of(fid)))
          .inOrder();

      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  Tuple5.of(PLATFORM_ID, USER_TYPE, promotedUid, QUERY_TYPE, xxhashHex("")),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_3_DAY : 1),
                  getExpiry(fid, 1)),
              RedisSink.hset(
                  Tuple5.of(PLATFORM_ID, USER_TYPE, promotedUid, QUERY_TYPE, xxhashHex("")),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_10_DAY : 1),
                  getExpiry(fid, 1)),
              RedisSink.hdel(
                  Tuple5.of(PLATFORM_ID, USER_TYPE, promotedUid, QUERY_TYPE, xxhashHex("")),
                  Tuple1.of(fid)))
          .inOrder();
      assertWithMessage(
              "Should not contain; countType=%s, isLogUser=%s, okhttpUid=%s, promotedUid=%s, isTimestamp=%s, fid=%s",
              countType, isLogUser, okhttpUid, promotedUid, isTimestamp, fid)
          .that(CollectingSink.output)
          .doesNotContain(
              RedisSink.hset(
                  Tuple5.of(
                      PLATFORM_ID,
                      USER_TYPE,
                      okhttpUid,
                      QUERY_TYPE,
                      xxhashHex(LOWER_CASE_QUERY_ONE)),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_2_HR : 1),
                  getExpiry(fid, 1)));
      assertWithMessage(
              "Should not contain; countType=%s, isLogUser=%s, okhttpUid=%s, promotedUid=%s, isTimestamp=%s,  fid=%s",
              countType, isLogUser, okhttpUid, promotedUid, isTimestamp, fid)
          .that(CollectingSink.output)
          .doesNotContain(
              RedisSink.hdel(
                  Tuple5.of(
                      PLATFORM_ID,
                      USER_TYPE,
                      okhttpUid,
                      QUERY_TYPE,
                      xxhashHex(LOWER_CASE_QUERY_ONE)),
                  Tuple1.of(fid)));
    }
  }

  /** A helper class for generating some test data. */
  private static class TestUser {

    /** Request Agent */
    final String RA;

    /** User Agent */
    final String UA;

    /** OS */
    final String OS;

    /** Device */
    final String DVC;

    /** User ID */
    final String UID;

    /** Log User ID */
    final String L_UID;

    public TestUser(String ra, String ua, String os, String dvc, String uid, String lUid) {
      this.RA = ra;
      this.UA = ua;
      this.OS = os;
      this.DVC = dvc;
      this.UID = uid;
      this.L_UID = lUid;
    }
  }

  /** SinkFunction used to collect stream output for test validation. */
  static class CollectingSink implements SinkFunction<RedisSinkCommand> {

    // TODO: this doesn't play nice with concurrency (multiple concurrent test runs) and difficult
    // to reuse.  As a consequence, we tagged the test target in the bazel target as "exclusive".
    // TODO: look into DataStream.executeAndCollect as a replacement
    public static final List<RedisSinkCommand> output =
        Collections.synchronizedList(new ArrayList<>());
    public long currentTimestamp = Long.MIN_VALUE;

    public void invoke(RedisSinkCommand value, Context ctx) {
      if (ctx != null && ctx.timestamp() != null) {
        currentTimestamp = ctx.timestamp();
      }
      LOGGER.trace(
          "address: output({}) this({}) timestamp: {} watermark= {} value: {}",
          System.identityHashCode(CollectingSink.output),
          System.identityHashCode(this),
          currentTimestamp,
          ctx.currentWatermark(),
          value);
      CollectingSink.output.add(value);
      LOGGER.debug("CollectingSink.output.size: {}", CollectingSink.output.size());
    }
  }
}
