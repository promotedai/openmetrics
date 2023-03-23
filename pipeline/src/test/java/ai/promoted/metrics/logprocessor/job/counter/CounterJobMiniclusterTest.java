package ai.promoted.metrics.logprocessor.job.counter;

import static ai.promoted.metrics.logprocessor.common.counter.Constants.QUERY_TYPE;
import static ai.promoted.metrics.logprocessor.common.counter.Constants.USER_TYPE;
import static ai.promoted.metrics.logprocessor.common.util.StringUtil.xxhash;
import static ai.promoted.metrics.logprocessor.common.util.StringUtil.xxhashHex;
import static ai.promoted.proto.event.ActionType.NAVIGATE;
import static com.google.common.truth.Truth.assertThat;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.logprocessor.common.counter.ContentEventDevice;
import ai.promoted.metrics.logprocessor.common.counter.ContentQueryEvent;
import ai.promoted.metrics.logprocessor.common.counter.FeatureId;
import ai.promoted.metrics.logprocessor.common.counter.GlobalEventDevice;
import ai.promoted.metrics.logprocessor.common.counter.LogUserEvent;
import ai.promoted.metrics.logprocessor.common.counter.QueryEvent;
import ai.promoted.metrics.logprocessor.common.counter.UserEvent;
import ai.promoted.metrics.logprocessor.common.functions.LastTimeAndCount;
import ai.promoted.metrics.logprocessor.common.functions.SlidingDailyCounter;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink;
import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobMiniclusterTest;
import ai.promoted.metrics.logprocessor.common.testing.MiniClusterExtension;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.delivery.internal.features.AggMetric;
import ai.promoted.proto.delivery.internal.features.CountFeatureMask;
import ai.promoted.proto.delivery.internal.features.CountType;
import ai.promoted.proto.delivery.internal.features.CountWindow;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.JoinedEvent;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
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
import org.apache.flink.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MiniClusterExtension.class)
// TODO Optimize the test cases to run only once and reuse results
public class CounterJobMiniclusterTest extends BaseJobMiniclusterTest<CounterJob> {

  private static final Logger LOGGER = LogManager.getLogger(CounterJobMiniclusterTest.class);
  private static final TypeInformation<JoinedEvent> joinedEventTypeInfo =
      TypeExtractor.getForClass(JoinedEvent.class);
  private static final TypeInformation<LogUserUser> logUserUserEventTypeInfo =
      TypeExtractor.getForClass(LogUserUser.class);
  private static final CollectingSink COLLECTING_SINK = new CollectingSink();
  private static final long DUR_10_MIN = Duration.ofMinutes(10).toMillis();
  private static final long DUR_30_MIN = Duration.ofMinutes(30).toMillis();
  private static final long DUR_2_HR = Duration.ofHours(2).toMillis();
  private static final long DUR_3_DAY = Duration.ofDays(3).toMillis();
  private static final long DUR_10_DAY = Duration.ofDays(10).toMillis();
  // Query string hex mapping:
  // "" => ef46db3751d8e999
  private static final String QUERY_ONE = "query one"; // QUERY_ONE => 28841ff692f4061a
  private static final String QUERY_TWO = "query two"; // QUERY_TWO => 41809fa5f00aefaa
  private static final TestUser PROMOTED =
      new TestUser(
          "promoted/980 CFNetwork/1240.0.4 Darwin/20.6.0", "promoted", "iOS", "eve", "log_eve");
  private static final TestUser OKHTTP =
      new TestUser("okhttp/3.14.9", "okhttp", "Other", "todd", "log_todd");
  private static final long PLATFORM_ID = 20L;
  // ============ Global Event Devices ================
  private static final GlobalEventDevice okHttpGlobalEventDevice =
      new GlobalEventDevice(PLATFORM_ID, AggMetric.COUNT_NAVIGATE.name(), OKHTTP.OS, OKHTTP.UA);
  private static final GlobalEventDevice promotedGlobalEventDevice =
      new GlobalEventDevice(PLATFORM_ID, AggMetric.COUNT_NAVIGATE.name(), PROMOTED.OS, PROMOTED.UA);
  // ============ Global Event Devices ================

  // ============ Event Devices ================
  private static final ContentEventDevice okHttpContentEventDevice10Min =
      new ContentEventDevice(
          PLATFORM_ID,
          genContentId(NAVIGATE, DUR_10_MIN),
          AggMetric.COUNT_NAVIGATE.name(),
          OKHTTP.OS,
          OKHTTP.UA);
  private static final ContentEventDevice okHttpContentEventDevice2Hr =
      new ContentEventDevice(
          PLATFORM_ID,
          genContentId(NAVIGATE, DUR_2_HR),
          AggMetric.COUNT_NAVIGATE.name(),
          OKHTTP.OS,
          OKHTTP.UA);
  private static final ContentEventDevice promotedHttpContentEventDevice30Min =
      new ContentEventDevice(
          PLATFORM_ID,
          genContentId(NAVIGATE, DUR_30_MIN),
          AggMetric.COUNT_NAVIGATE.name(),
          PROMOTED.OS,
          PROMOTED.UA);
  private static final ContentEventDevice promotedHttpContentEventDevice3Day =
      new ContentEventDevice(
          PLATFORM_ID,
          genContentId(NAVIGATE, DUR_3_DAY),
          AggMetric.COUNT_NAVIGATE.name(),
          PROMOTED.OS,
          PROMOTED.UA);
  private static final ContentEventDevice promotedHttpContentEventDevice10Day =
      new ContentEventDevice(
          PLATFORM_ID,
          genContentId(NAVIGATE, DUR_10_DAY),
          AggMetric.COUNT_NAVIGATE.name(),
          PROMOTED.OS,
          PROMOTED.UA);
  // ============ Event Devices ================

  // ============ Query Events ================
  private static final QueryEvent queryEventOne =
      new QueryEvent(PLATFORM_ID, xxhash(QUERY_ONE), AggMetric.COUNT_NAVIGATE.name());
  private static final QueryEvent queryEventTwo =
      new QueryEvent(PLATFORM_ID, xxhash(QUERY_TWO), AggMetric.COUNT_NAVIGATE.name());
  private static final QueryEvent queryEventEmpty =
      new QueryEvent(PLATFORM_ID, xxhash(""), AggMetric.COUNT_NAVIGATE.name());
  // ============ Query Events ================

  // ============ Content Query Events ================
  private static final ContentQueryEvent contentQueryEventTwo10Min =
      new ContentQueryEvent(
          PLATFORM_ID,
          xxhash(QUERY_TWO),
          genContentId(NAVIGATE, DUR_10_MIN),
          AggMetric.COUNT_NAVIGATE.name());
  private static final ContentQueryEvent contentQueryEventTwo30Min =
      new ContentQueryEvent(
          PLATFORM_ID,
          xxhash(QUERY_TWO),
          genContentId(NAVIGATE, DUR_30_MIN),
          AggMetric.COUNT_NAVIGATE.name());
  private static final ContentQueryEvent contentQueryEventOne2Hr =
      new ContentQueryEvent(
          PLATFORM_ID,
          xxhash(QUERY_ONE),
          genContentId(NAVIGATE, DUR_2_HR),
          AggMetric.COUNT_NAVIGATE.name());
  private static final ContentQueryEvent contentQueryEventEmpty3Day =
      new ContentQueryEvent(
          PLATFORM_ID,
          xxhash(""),
          genContentId(NAVIGATE, DUR_3_DAY),
          AggMetric.COUNT_NAVIGATE.name());

  private static final ContentQueryEvent contentQueryEventEmpty10Day =
      new ContentQueryEvent(
          PLATFORM_ID,
          xxhash(""),
          genContentId(NAVIGATE, DUR_10_DAY),
          AggMetric.COUNT_NAVIGATE.name());
  // ============ Content Query Events ================

  // ============ User/LogUser Events ================
  private static final UserEvent okHttpUserEvent =
      new UserEvent(PLATFORM_ID, OKHTTP.UID, AggMetric.COUNT_NAVIGATE.name());
  private static final LogUserEvent okHttpLogUserEvent =
      new LogUserEvent(PLATFORM_ID, OKHTTP.L_UID, AggMetric.COUNT_NAVIGATE.name());
  private static final UserEvent promotedUserEvent =
      new UserEvent(PLATFORM_ID, PROMOTED.UID, AggMetric.COUNT_NAVIGATE.name());
  private static final LogUserEvent promotedLogUserEvent =
      new LogUserEvent(PLATFORM_ID, PROMOTED.L_UID, AggMetric.COUNT_NAVIGATE.name());
  // ============ User/LogUser Events ================

  private static long getExpiry(long fid) {
    CountWindow window =
        CountWindow.forNumber(Math.toIntExact(fid & CountFeatureMask.WINDOW.getNumber()));
    switch (window) {
      case DAY_30:
        return SlidingDailyCounter.EXPIRE_TTL_SECONDS;
      case DAY_90:
        throw new IllegalArgumentException(
            "use the getExpiry(fid, count) for last user event counts");
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

  private static JoinedEvent genJoinedEvent(
      Action action,
      @Nullable String logUserId,
      @Nullable String userAgent,
      @Nullable String query) {
    JoinedEvent.Builder builder =
        FlatUtil.setFlatAction(
            JoinedEvent.newBuilder().setTiming(action.getTiming()), action, (tag, error) -> {});

    if (!StringUtils.isNullOrWhitespaceOnly(logUserId)) {
      builder.getUserBuilder().getUserInfoBuilder().setLogUserId(logUserId);
    }
    if (!StringUtils.isNullOrWhitespaceOnly(userAgent)) {
      builder.getRequestBuilder().getDeviceBuilder().getBrowserBuilder().setUserAgent(userAgent);
    }
    if (!StringUtils.isNullOrWhitespaceOnly(query)) {
      builder.getRequestBuilder().setSearchQuery(query);
    }

    LOGGER.trace(builder);
    return builder.build();
  }

  private static Action genAction(ActionType actionType, long eventTime) {
    Action.Builder builder =
        Action.newBuilder()
            .setPlatformId(PLATFORM_ID)
            .setActionType(actionType)
            .setContentId(genContentId(actionType, eventTime));
    builder.getTimingBuilder().setEventApiTimestamp(eventTime);
    return builder.build();
  }

  private static String genContentId(ActionType actionType, long eventTime) {
    return String.format("%s @ %s", actionType, eventTime);
  }

  private static LogUserUser genLogUserUser(
      String userId, String logUserId, long timestamp, long platformId) {
    return LogUserUser.newBuilder()
        .setUserId(userId)
        .setLogUserId(logUserId)
        .setEventApiTimestamp(timestamp)
        .setPlatformId(platformId)
        .build();
  }

  @AfterEach
  public void clearSink() {
    CollectingSink.output.clear();
  }

  @Override
  protected CounterJob createJob() {
    CounterJob job = new CounterJob();
    job.counterOutputStartTimestamp = 0;
    job.s3.rootPath = tempDir.getAbsolutePath();
    // Checkpoint more frequently so we don't hit part files.
    job.checkpointInterval = Duration.ofSeconds(1);
    job.checkpointTimeout = Duration.ofSeconds(1);
    job.configureExecutionEnvironment(env, 1, 0);
    job.counterBackfillBuffer = Duration.ZERO;
    job.searchQueryWindowSize = Duration.ofDays(1);
    job.searchQueryWindowSlide = Duration.ofMinutes(1);
    job.searchQueryWindowMinThreshold = 2;
    return job;
  }

  private void setupCommonTestCase(CounterJob job) {
    // In reality, there is a difference between the user and non-user flat event streams, but it's
    // fine to reuse the stream for the purposes of testing.
    //
    CollectionSource<JoinedEvent> joinedEventsInput =
        new CollectionSource<>(
            ImmutableList.of(
                genJoinedEvent(genAction(NAVIGATE, DUR_10_MIN), OKHTTP.L_UID, OKHTTP.RA, QUERY_TWO),
                genJoinedEvent(
                    genAction(NAVIGATE, DUR_30_MIN), PROMOTED.L_UID, PROMOTED.RA, QUERY_TWO),
                genJoinedEvent(genAction(NAVIGATE, DUR_2_HR), OKHTTP.L_UID, OKHTTP.RA, QUERY_ONE),
                genJoinedEvent(genAction(NAVIGATE, DUR_3_DAY), PROMOTED.L_UID, PROMOTED.RA, ""),
                genJoinedEvent(genAction(NAVIGATE, DUR_10_DAY), PROMOTED.L_UID, PROMOTED.RA, "")),
            e -> e.getTiming().getEventApiTimestamp());

    CollectionSource<LogUserUser> logUserUserInput =
        new CollectionSource<>(
            ImmutableList.of(
                genLogUserUser(OKHTTP.UID, OKHTTP.L_UID, DUR_10_MIN - 1, PLATFORM_ID),
                genLogUserUser(PROMOTED.UID, PROMOTED.L_UID, DUR_30_MIN, PLATFORM_ID),
                genLogUserUser(OKHTTP.UID, OKHTTP.L_UID, DUR_2_HR, PLATFORM_ID),
                genLogUserUser(PROMOTED.UID, PROMOTED.L_UID, DUR_3_DAY, PLATFORM_ID),
                genLogUserUser("", "", DUR_10_DAY, PLATFORM_ID) // for watermark only
                ),
            LogUserUser::getEventApiTimestamp);

    job.defineJob(
        fromCollectionSource(
            env, "joined-events", joinedEventsInput, joinedEventTypeInfo, JoinedEvent::getTiming),
        fromCollectionSource(
            env,
            "log-user-user-events",
            logUserUserInput,
            logUserUserEventTypeInfo,
            e -> Timing.newBuilder().setEventApiTimestamp(e.getEventApiTimestamp()).build()),
        COLLECTING_SINK);
  }

  @Test
  void basicCounts() throws Exception {
    CounterJob job = createJob();
    setupCommonTestCase(job);
    waitForDone(env.execute("counter"));

    // Rather than test all the redis commands, just check that certain values were set.
    // Globals - note we expect globals to never expire

    for (Tuple2<Tuple, Integer> redisFieldAndCnt :
        ImmutableList.of(
            Tuple2.of(okHttpGlobalEventDevice.getHashField(1, HOURS), 1),
            Tuple2.of(okHttpGlobalEventDevice.getHashField(1, DAYS), 2),
            Tuple2.of(okHttpGlobalEventDevice.getHashField(7, DAYS), 2),
            Tuple2.of(okHttpGlobalEventDevice.getHashField(30, DAYS), 2))) {

      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  okHttpGlobalEventDevice.getHashKey(),
                  redisFieldAndCnt.f0,
                  redisFieldAndCnt.f1,
                  -1),
              // We will always get a 0 result since watermark=Long.MAX_VALUE will purge everything.
              RedisSink.hdel(okHttpGlobalEventDevice.getHashKey(), redisFieldAndCnt.f0))
          .inOrder();

      assertThat(CollectingSink.output)
          .doesNotContain(
              RedisSink.hset(
                  okHttpGlobalEventDevice.getHashKey(),
                  redisFieldAndCnt.f0,
                  redisFieldAndCnt.f1 + 1,
                  -1));
    }

    for (Tuple2<Tuple, Integer> redisFieldAndCnt :
        ImmutableList.of(
            Tuple2.of(promotedGlobalEventDevice.getHashField(1, HOURS), 1),
            Tuple2.of(promotedGlobalEventDevice.getHashField(1, DAYS), 1),
            Tuple2.of(promotedGlobalEventDevice.getHashField(7, DAYS), 2),
            Tuple2.of(promotedGlobalEventDevice.getHashField(30, DAYS), 3))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  promotedGlobalEventDevice.getHashKey(),
                  redisFieldAndCnt.f0,
                  redisFieldAndCnt.f1,
                  -1),
              RedisSink.hdel(promotedGlobalEventDevice.getHashKey(), redisFieldAndCnt.f0))
          .inOrder();

      assertThat(CollectingSink.output)
          .doesNotContain(
              RedisSink.hset(
                  promotedGlobalEventDevice.getHashKey(),
                  redisFieldAndCnt.f0,
                  redisFieldAndCnt.f1 + 1,
                  -1));
    }

    // Item: everything should be unique since they are id'd by timestamp

    for (Tuple hashField :
        ImmutableList.of(
            okHttpContentEventDevice10Min.getHashField(1, HOURS),
            okHttpContentEventDevice10Min.getHashField(1, DAYS),
            okHttpContentEventDevice10Min.getHashField(7, DAYS),
            okHttpContentEventDevice10Min.getHashField(30, DAYS))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  okHttpContentEventDevice10Min.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(2))),
              RedisSink.hset(
                  okHttpContentEventDevice2Hr.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(2))))
          .inOrder();
      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  okHttpContentEventDevice10Min.getHashKey(),
                  hashField,
                  2,
                  getExpiry(hashField.getField(2))),
              RedisSink.hset(
                  okHttpContentEventDevice2Hr.getHashKey(),
                  hashField,
                  2,
                  getExpiry(hashField.getField(2))));
    }

    for (Tuple hashField :
        ImmutableList.of(
            promotedHttpContentEventDevice30Min.getHashField(1, HOURS),
            promotedHttpContentEventDevice30Min.getHashField(1, DAYS),
            promotedHttpContentEventDevice30Min.getHashField(7, DAYS),
            promotedHttpContentEventDevice30Min.getHashField(30, DAYS))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  promotedHttpContentEventDevice30Min.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(2))),
              RedisSink.hset(
                  promotedHttpContentEventDevice3Day.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(2))),
              RedisSink.hset(
                  promotedHttpContentEventDevice10Day.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(2))),
              RedisSink.hdel(promotedHttpContentEventDevice10Day.getHashKey(), hashField))
          .inOrder();

      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  promotedHttpContentEventDevice30Min.getHashKey(),
                  hashField,
                  2,
                  getExpiry(hashField.getField(2))),
              RedisSink.hset(
                  promotedHttpContentEventDevice3Day.getHashKey(),
                  hashField,
                  2,
                  getExpiry(hashField.getField(2))),
              RedisSink.hset(
                  promotedHttpContentEventDevice10Day.getHashKey(),
                  hashField,
                  2,
                  getExpiry(hashField.getField(2))));
    }
    // TODO enable this check when the results become deterministic
    //        assertThat(CollectingSink.output).hasSize(335);
  }

  @Test
  void basicCounts_buffered() throws Exception {
    CounterJob job = createJob();

    // Because the entire input window is in the buffer, only 0 values should be output.
    job.counterBackfillBuffer = Duration.ofDays(14);
    setupCommonTestCase(job);
    waitForDone(env.execute("counter"));

    // Rather than test all the redis commands, just check that certain values were set.
    // Globals - note we expect globals to never expire
    for (Tuple hashField :
        ImmutableList.of(
            okHttpGlobalEventDevice.getHashField(1, HOURS),
            okHttpGlobalEventDevice.getHashField(1, DAYS),
            okHttpGlobalEventDevice.getHashField(7, DAYS),
            okHttpGlobalEventDevice.getHashField(30, DAYS))) {
      assertThat(CollectingSink.output)
          .contains(RedisSink.hdel(okHttpGlobalEventDevice.getHashKey(), hashField));

      assertThat(CollectingSink.output)
          .doesNotContain(RedisSink.hset(okHttpGlobalEventDevice, hashField, 1, -1));
    }
    for (Tuple hashField :
        ImmutableList.of(
            promotedGlobalEventDevice.getHashField(1, HOURS),
            promotedGlobalEventDevice.getHashField(1, DAYS),
            promotedGlobalEventDevice.getHashField(7, DAYS),
            promotedGlobalEventDevice.getHashField(30, DAYS))) {
      assertThat(CollectingSink.output)
          .contains(RedisSink.hdel(promotedGlobalEventDevice.getHashKey(), hashField));

      assertThat(CollectingSink.output)
          .doesNotContain(RedisSink.hset(promotedGlobalEventDevice.getHashKey(), hashField, 1, -1));
    }

    // Items
    for (Tuple hashField :
        ImmutableList.of(
            okHttpContentEventDevice10Min.getHashField(1, HOURS),
            okHttpContentEventDevice10Min.getHashField(1, DAYS),
            okHttpContentEventDevice10Min.getHashField(7, DAYS),
            okHttpContentEventDevice10Min.getHashField(30, DAYS))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hdel(okHttpContentEventDevice10Min.getHashKey(), hashField),
              RedisSink.hdel(okHttpContentEventDevice2Hr.getHashKey(), hashField));

      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  okHttpContentEventDevice10Min.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(2))),
              RedisSink.hset(
                  okHttpContentEventDevice2Hr.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(2))));
    }
    for (Tuple hashField :
        ImmutableList.of(
            promotedHttpContentEventDevice10Day.getHashField(1, HOURS),
            promotedHttpContentEventDevice10Day.getHashField(1, DAYS),
            promotedHttpContentEventDevice10Day.getHashField(7, DAYS),
            promotedHttpContentEventDevice10Day.getHashField(30, DAYS))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hdel(promotedHttpContentEventDevice30Min.getHashKey(), hashField),
              RedisSink.hdel(promotedHttpContentEventDevice3Day.getHashKey(), hashField),
              RedisSink.hdel(promotedHttpContentEventDevice10Day.getHashKey(), hashField));

      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  promotedHttpContentEventDevice30Min.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(2))),
              RedisSink.hset(
                  promotedHttpContentEventDevice3Day.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(2))),
              RedisSink.hset(
                  promotedHttpContentEventDevice10Day.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(2))));
    }
  }

  @Test
  void queryCounts() throws Exception {
    CounterJob job = createJob();
    setupCommonTestCase(job);
    waitForDone(env.execute("counter"));

    // Query hourly
    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(queryEventTwo.getHashKey(), queryEventTwo.getHashField(1, HOURS), 2, 0),
            RedisSink.hdel(queryEventTwo.getHashKey(), queryEventTwo.getHashField(1, HOURS)))
        .inOrder();

    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                queryEventEmpty.getHashKey(), queryEventEmpty.getHashField(1, HOURS), 1, 0),
            RedisSink.hdel(queryEventEmpty.getHashKey(), queryEventEmpty.getHashField(1, HOURS)))
        .inOrder();

    assertThat(CollectingSink.output)
        .containsNoneOf(
            RedisSink.hset(queryEventOne.getHashKey(), queryEventOne.getHashField(1, HOURS), 1, 0),
            RedisSink.hdel(queryEventOne.getHashKey(), queryEventOne.getHashField(1, HOURS)));

    // Query daily
    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(queryEventTwo.getHashKey(), queryEventTwo.getHashField(1, DAYS), 2, 0),
            RedisSink.hdel(queryEventTwo.getHashKey(), queryEventTwo.getHashField(1, DAYS)))
        .inOrder();
    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(queryEventTwo.getHashKey(), queryEventTwo.getHashField(1, DAYS), 2, 0),
            RedisSink.hdel(queryEventTwo.getHashKey(), queryEventTwo.getHashField(1, DAYS)))
        .inOrder();
    assertThat(CollectingSink.output)
        .containsNoneOf(
            RedisSink.hset(queryEventOne.getHashKey(), queryEventOne.getHashField(1, DAYS), 1, 0),
            RedisSink.hdel(queryEventOne.getHashKey(), queryEventOne.getHashField(1, DAYS)));

    // Query 7d
    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(queryEventTwo.getHashKey(), queryEventTwo.getHashField(7, DAYS), 2, 0),
            RedisSink.hdel(queryEventTwo.getHashKey(), queryEventTwo.getHashField(7, DAYS)))
        .inOrder();
    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                queryEventEmpty.getHashKey(), queryEventEmpty.getHashField(7, DAYS), 1, 0),
            RedisSink.hdel(queryEventEmpty.getHashKey(), queryEventEmpty.getHashField(7, DAYS)))
        .inOrder();
    assertThat(CollectingSink.output)
        .containsNoneOf(
            RedisSink.hset(queryEventOne.getHashKey(), queryEventOne.getHashField(7, DAYS), 1, 0),
            RedisSink.hdel(queryEventOne.getHashKey(), queryEventOne.getHashField(7, DAYS)));

    // Query 30d needs to be broken up due to likely bug in truth's inOrder evaluation.
    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                queryEventTwo.getHashKey(),
                queryEventTwo.getHashField(30, DAYS),
                2,
                getExpiry(queryEventTwo.getHashField(30, DAYS).getField(0))),
            RedisSink.hdel(queryEventTwo.getHashKey(), queryEventTwo.getHashField(30, DAYS)))
        .inOrder();

    assertThat(CollectingSink.output)
        .containsAtLeast(
            RedisSink.hset(
                queryEventEmpty.getHashKey(),
                queryEventEmpty.getHashField(30, DAYS),
                2,
                getExpiry(queryEventEmpty.getHashField(30, DAYS).getField(0))),
            RedisSink.hdel(queryEventEmpty.getHashKey(), queryEventEmpty.getHashField(30, DAYS)))
        .inOrder();
    assertThat(CollectingSink.output)
        .doesNotContain(
            RedisSink.hset(
                queryEventOne.getHashKey(),
                queryEventOne.getHashField(30, DAYS),
                1,
                getExpiry(queryEventOne.getHashField(30, DAYS).getField(0))));

    // Item x Query
    for (Tuple hashField :
        ImmutableList.of(
            contentQueryEventTwo10Min.getHashField(1, HOURS),
            contentQueryEventTwo10Min.getHashField(1, DAYS),
            contentQueryEventTwo10Min.getHashField(7, DAYS),
            contentQueryEventTwo10Min.getHashField(30, DAYS))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  contentQueryEventTwo10Min.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(0))),
              RedisSink.hset(
                  contentQueryEventTwo30Min.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(0))))
          .inOrder();
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  contentQueryEventEmpty3Day.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(0))),
              RedisSink.hset(
                  contentQueryEventEmpty10Day.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(0))))
          .inOrder();
      assertThat(CollectingSink.output)
          .doesNotContain(
              RedisSink.hset(
                  contentQueryEventOne2Hr.getHashKey(),
                  hashField,
                  1,
                  getExpiry(hashField.getField(0))));
    }
  }

  @Test
  void userCounts() throws Exception {
    CounterJob job = createJob();
    setupCommonTestCase(job);
    waitForDone(env.execute("counter"));

    // User: todd
    for (Tuple3<Tuple, Tuple, Integer> test :
        ImmutableList.of(
            Tuple3.of(
                okHttpLogUserEvent.getHashField(1, HOURS),
                okHttpUserEvent.getHashField(1, HOURS),
                1),
            Tuple3.of(
                okHttpLogUserEvent.getHashField(1, DAYS), okHttpUserEvent.getHashField(1, DAYS), 2),
            Tuple3.of(
                okHttpLogUserEvent.getHashField(7, DAYS), okHttpUserEvent.getHashField(7, DAYS), 2),
            Tuple3.of(
                okHttpLogUserEvent.getHashField(30, DAYS),
                okHttpUserEvent.getHashField(30, DAYS),
                2))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  okHttpLogUserEvent.getHashKey(),
                  test.f0,
                  test.f2,
                  getExpiry(test.f0.getField(0))),
              RedisSink.hdel(okHttpLogUserEvent.getHashKey(), test.f0))
          .inOrder();
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  okHttpUserEvent.getHashKey(), test.f1, test.f2, getExpiry(test.f1.getField(0))),
              RedisSink.hdel(okHttpUserEvent.getHashKey(), test.f1))
          .inOrder();

      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  okHttpLogUserEvent.getHashKey(),
                  test.f0,
                  test.f2 + 1,
                  getExpiry(test.f0.getField(0))),
              RedisSink.hset(
                  okHttpUserEvent.getHashKey(),
                  test.f1,
                  test.f2 + 1,
                  getExpiry(test.f1.getField(0))));
    }

    // User: eve
    for (Tuple3<Tuple, Tuple, Integer> test :
        ImmutableList.of(
            Tuple3.of(
                promotedLogUserEvent.getHashField(1, HOURS),
                promotedUserEvent.getHashField(1, HOURS),
                1),
            Tuple3.of(
                promotedLogUserEvent.getHashField(1, DAYS),
                promotedUserEvent.getHashField(1, DAYS),
                1),
            Tuple3.of(
                promotedLogUserEvent.getHashField(7, DAYS),
                promotedUserEvent.getHashField(7, DAYS),
                2),
            Tuple3.of(
                promotedLogUserEvent.getHashField(30, DAYS),
                promotedUserEvent.getHashField(30, DAYS),
                3))) {
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  promotedLogUserEvent.getHashKey(),
                  test.f0,
                  test.f2,
                  getExpiry(test.f0.getField(0))),
              RedisSink.hdel(promotedLogUserEvent.getHashKey(), test.f0))
          .inOrder();
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  promotedUserEvent.getHashKey(), test.f1, test.f2, getExpiry(test.f1.getField(0))),
              RedisSink.hdel(promotedUserEvent.getHashKey(), test.f1))
          .inOrder();

      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  promotedLogUserEvent.getHashKey(),
                  test.f0,
                  test.f2 + 1,
                  getExpiry(test.f0.getField(0))),
              RedisSink.hset(
                  promotedUserEvent.getHashKey(),
                  test.f1,
                  test.f2 + 1,
                  getExpiry(test.f1.getField(0))));
    }
  }

  @Test
  void logUserItemLastTime() throws Exception {
    CounterJob job = createJob();
    setupCommonTestCase(job);
    waitForDone(env.execute("counter"));
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
    setupCommonTestCase(job);
    waitForDone(env.execute("counter"));
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
                  Tuple5.of(PLATFORM_ID, USER_TYPE, okhttpUid, QUERY_TYPE, xxhashHex(QUERY_TWO)),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_10_MIN : 1),
                  getExpiry(fid, 1)),
              RedisSink.hdel(
                  Tuple5.of(PLATFORM_ID, USER_TYPE, okhttpUid, QUERY_TYPE, xxhashHex(QUERY_TWO)),
                  Tuple1.of(fid)))
          .inOrder();
      assertThat(CollectingSink.output)
          .containsAtLeast(
              RedisSink.hset(
                  Tuple5.of(PLATFORM_ID, USER_TYPE, promotedUid, QUERY_TYPE, xxhashHex(QUERY_TWO)),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_30_MIN : 1),
                  getExpiry(fid, 1)),
              RedisSink.hdel(
                  Tuple5.of(PLATFORM_ID, USER_TYPE, promotedUid, QUERY_TYPE, xxhashHex(QUERY_TWO)),
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
      assertThat(CollectingSink.output)
          .containsNoneOf(
              RedisSink.hset(
                  Tuple5.of(PLATFORM_ID, USER_TYPE, okhttpUid, QUERY_TYPE, xxhashHex(QUERY_ONE)),
                  Tuple1.of(fid),
                  Tuple1.of(isTimestamp ? DUR_2_HR : 1),
                  getExpiry(fid, 1)),
              RedisSink.hdel(
                  Tuple5.of(PLATFORM_ID, USER_TYPE, okhttpUid, QUERY_TYPE, xxhashHex(QUERY_ONE)),
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

    /** User ID */
    final String UID;

    /** Log User ID */
    final String L_UID;

    public TestUser(String ra, String ua, String os, String uid, String lUid) {
      this.RA = ra;
      this.UA = ua;
      this.OS = os;
      this.UID = uid;
      this.L_UID = lUid;
    }
  }

  /** SinkFunction used to collect stream output for test validation. */
  static class CollectingSink implements SinkFunction<RedisSink.Command> {

    // TODO: this doesn't play nice with concurrency (multiple concurrent test runs) and difficult
    // to reuse.  As a consequence, we tagged the test target in the bazel target as "exclusive".
    // TODO: look into DataStream.executeAndCollect as a replacement
    public static final List<RedisSink.Command> output =
        Collections.synchronizedList(new ArrayList<>());
    public long currentTimestamp = Long.MIN_VALUE;

    public void invoke(RedisSink.Command value, Context ctx) {
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
