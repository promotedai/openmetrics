package ai.promoted.metrics.logprocessor.job.counter;

import ai.promoted.metrics.logprocessor.common.functions.LastTimeAndCount;
import ai.promoted.metrics.logprocessor.common.functions.SlidingDailyCounter;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink;
import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobMiniclusterTest;
import ai.promoted.metrics.logprocessor.common.testing.MiniClusterExtension;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.delivery.internal.features.AggMetric;
import ai.promoted.proto.delivery.internal.features.CountFeatureMask;
import ai.promoted.proto.delivery.internal.features.CountType;
import ai.promoted.proto.delivery.internal.features.CountWindow;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.JoinedEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static ai.promoted.metrics.logprocessor.common.util.StringUtil.xxhashHex;
import static ai.promoted.metrics.logprocessor.job.counter.CounterKeys.QUERY_TYPE;
import static ai.promoted.metrics.logprocessor.job.counter.CounterKeys.USER_TYPE;
import static com.google.common.truth.Truth.assertThat;

@ExtendWith(MiniClusterExtension.class)
public class CounterJobMiniclusterTest extends BaseJobMiniclusterTest<CounterJob> {
  private static final Logger LOGGER = LogManager.getLogger(CounterJobMiniclusterTest.class);
  private static final TypeInformation<JoinedEvent> joinedEventTypeInfo = TypeExtractor.getForClass(JoinedEvent.class);

  private static final CollectingSink COLLECTING_SINK = new CollectingSink();

  @AfterEach
  public void clearSink() {
    CollectingSink.output.clear();
  }

  @Override
  protected CounterJob createJob() {
    CounterJob job = new CounterJob();
    job.counterOutputStartTimestamp = 0;
    job.s3FileOutput.s3OutputDirectory = tempDir.getAbsolutePath();
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
    // Query string hex mapping:
    // "" => ef46db3751d8e999
    // "query one" => 28841ff692f4061a
    // "query two" => 41809fa5f00aefaa
    CollectionSource<JoinedEvent> input = new CollectionSource<>(
        ImmutableList.of(
          joinedEvent(createAction(ActionType.NAVIGATE, Duration.ofMinutes(10).toMillis() + 1), "query two"),
          joinedEvent(createAction(ActionType.NAVIGATE, Duration.ofMinutes(30).toMillis()), "query two"),
          joinedEvent(createAction(ActionType.NAVIGATE, Duration.ofHours(2).toMillis() + 1), "query one"),
          joinedEvent(createAction(ActionType.NAVIGATE, Duration.ofDays(3).toMillis()), ""),
          joinedEvent(createAction(ActionType.NAVIGATE, Duration.ofDays(10).toMillis()), "")),
        e -> e.getTiming().getEventApiTimestamp());

    job.defineJob(
        fromCollectionSource(env, "joined-events", input, joinedEventTypeInfo, JoinedEvent::getTiming),
        fromCollectionSource(env, "joined-user-events", input, joinedEventTypeInfo, JoinedEvent::getTiming),
        COLLECTING_SINK);
  }

  @Test
  void basicCounts() throws Exception {
    CounterJob job = createJob();
    setupCommonTestCase(job);
    waitForDone(env.execute("counter"));

    // Rather than test all the redis commands, just check that certain values were set.
    // Globals - note we expect globals to never expire
    for (Tuple2<Long, Integer> test : ImmutableList.of(
          Tuple2.of(FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 1, "h"), 1),
          Tuple2.of(FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 1, "d"), 2),
          Tuple2.of(FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 7, "d"), 2),
          Tuple2.of(FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 30, "d"), 2))) {
      assertThat(CollectingSink.output).containsAtLeast(
          RedisSink.hset(Tuple1.of(0), Tuple3.of("Other", "okhttp", test.f0), test.f1, -1),
          RedisSink.hset(Tuple1.of(0), Tuple3.of("Other", "okhttp", test.f0), 0, -1))
        .inOrder();

      assertThat(CollectingSink.output).doesNotContain(
          RedisSink.hset(Tuple1.of(0), Tuple3.of("Other", "okhttp", test.f0), test.f1 + 1, -1));
    }
    for (Tuple2<Long, Integer> test : ImmutableList.of(
          Tuple2.of(FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 1, "h"), 1),
          Tuple2.of(FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 1, "d"), 1),
          Tuple2.of(FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 7, "d"), 2),
          Tuple2.of(FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 30, "d"), 3))) {
      assertThat(CollectingSink.output).containsAtLeast(
          RedisSink.hset(Tuple1.of(0), Tuple3.of("iOS", "ccc", test.f0), test.f1, -1),
          RedisSink.hset(Tuple1.of(0), Tuple3.of("iOS", "ccc", test.f0), 0, -1))
        .inOrder();

      assertThat(CollectingSink.output).doesNotContain(
          RedisSink.hset(Tuple1.of(0), Tuple3.of("iOS", "ccc", test.f0), test.f1 + 1, -1));
    }

    // Item: everything should be unique since they are id'd by timestamp
    for (Long fid : ImmutableList.of(
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 1, "h"),
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 1, "d"),
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 7, "d"),
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 30, "d"))) {
      assertThat(CollectingSink.output).containsAtLeast(
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 600001"), Tuple3.of("Other", "okhttp", fid), 1, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 1800000"), Tuple3.of("iOS", "ccc", fid), 1, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 7200001"), Tuple3.of("Other", "okhttp", fid), 1, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 259200000"), Tuple3.of("iOS", "ccc", fid), 1, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 864000000"), Tuple3.of("iOS", "ccc", fid), 1, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 864000000"), Tuple3.of("iOS", "ccc", fid), 0, getExpiry(fid)))
        .inOrder();

      assertThat(CollectingSink.output).containsNoneOf(
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 600001"), Tuple3.of("Other", "okhttp", fid), 2, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 1800000"), Tuple3.of("iOS", "ccc", fid), 2, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 7200001"), Tuple3.of("Other", "okhttp", fid), 2, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 259200000"), Tuple3.of("iOS", "ccc", fid), 2, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 864000000"), Tuple3.of("iOS", "ccc", fid), 2, getExpiry(fid)));
    }
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
    for (long fid : ImmutableList.of(
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 1, "h"),
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 1, "d"),
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 7, "d"),
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 30, "d"))) {
      assertThat(CollectingSink.output).contains(
          RedisSink.hset(Tuple1.of(0), Tuple3.of("Other", "okhttp", fid), 0, -1));

      assertThat(CollectingSink.output).doesNotContain(
          RedisSink.hset(Tuple1.of(0), Tuple3.of("Other", "okhttp", fid), 1, -1));
    }
    for (long fid : ImmutableList.of(
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 1, "h"),
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 1, "d"),
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 7, "d"),
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 30, "d"))) {
      assertThat(CollectingSink.output).contains(
          RedisSink.hset(Tuple1.of(0), Tuple3.of("iOS", "ccc", fid), 0, -1));

      assertThat(CollectingSink.output).doesNotContain(
          RedisSink.hset(Tuple1.of(0), Tuple3.of("iOS", "ccc", fid), 1, -1));
    }

    // Items
    for (Long fid : ImmutableList.of(
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 1, "h"),
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 1, "d"),
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 7, "d"),
          FeatureId.itemDeviceCount(AggMetric.COUNT_NAVIGATE, 30, "d"))) {
      assertThat(CollectingSink.output).containsAtLeast(
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 600001"), Tuple3.of("Other", "okhttp", fid), 0, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 1800000"), Tuple3.of("iOS", "ccc", fid), 0, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 7200001"), Tuple3.of("Other", "okhttp", fid), 0, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 259200000"), Tuple3.of("iOS", "ccc", fid), 0, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 864000000"), Tuple3.of("iOS", "ccc", fid), 0, getExpiry(fid)));

      assertThat(CollectingSink.output).containsNoneOf(
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 600001"), Tuple3.of("Other", "okhttp", fid), 1, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 1800000"), Tuple3.of("iOS", "ccc", fid), 1, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 7200001"), Tuple3.of("Other", "okhttp", fid), 1, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 259200000"), Tuple3.of("iOS", "ccc", fid), 1, getExpiry(fid)),
          RedisSink.hset(Tuple2.of(0, "NAVIGATE @ 864000000"), Tuple3.of("iOS", "ccc", fid), 1, getExpiry(fid)));
    }
  }

  @Test
  void queryCounts() throws Exception {
    CounterJob job = createJob();
    setupCommonTestCase(job);
    waitForDone(env.execute("counter"));

    // Query hourly
    long fid = FeatureId.queryCount(AggMetric.COUNT_NAVIGATE, 1, "h");
    /* TODO: re-enable this portion once we can guarantee the query counting is always complete before hourly output
    assertThat(CollectingSink.output).containsAtLeast(
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query two")), Tuple1.of(fid), 2, 0),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query two")), Tuple1.of(fid), 0, 0));
      //.inOrder();
    */
    assertThat(CollectingSink.output).containsNoneOf(
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query one")), Tuple1.of(fid), 1, 0),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query one")), Tuple1.of(fid), 0, 0),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("")), Tuple1.of(fid), 1, 0),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("")), Tuple1.of(fid), 0, 0));

    // Query daily
    fid = FeatureId.queryCount(AggMetric.COUNT_NAVIGATE, 1, "d");
    assertThat(CollectingSink.output).containsAtLeast(
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query two")), Tuple1.of(fid), 2, 0),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query two")), Tuple1.of(fid), 0, 0))
      .inOrder();
    assertThat(CollectingSink.output).containsNoneOf(
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query one")), Tuple1.of(fid), 1, 0),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query one")), Tuple1.of(fid), 0, 0),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("")), Tuple1.of(fid), 1, 0),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("")), Tuple1.of(fid), 0, 0));

    // Query 7d
    fid = FeatureId.queryCount(AggMetric.COUNT_NAVIGATE, 7, "d");
    assertThat(CollectingSink.output).containsAtLeast(
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query two")), Tuple1.of(fid), 2, 0),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query two")), Tuple1.of(fid), 0, 0))
      .inOrder();
    assertThat(CollectingSink.output).containsNoneOf(
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query one")), Tuple1.of(fid), 1, 0),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query one")), Tuple1.of(fid), 0, 0),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("")), Tuple1.of(fid), 1, 0),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("")), Tuple1.of(fid), 0, 0));

    // Query 30d (needs to be broken up due to likely bug in truth's inOrder evaluation.
    fid = FeatureId.queryCount(AggMetric.COUNT_NAVIGATE, 30, "d");
    assertThat(CollectingSink.output).containsAtLeast(
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query two")), Tuple1.of(fid), 2, getExpiry(fid)),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query two")), Tuple1.of(fid), 0, getExpiry(fid)))
      .inOrder();
    assertThat(CollectingSink.output).containsNoneOf(
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("query one")), Tuple1.of(fid), 1, getExpiry(fid)),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("")), Tuple1.of(fid), 0, getExpiry(fid)),
        RedisSink.hset(Tuple3.of(0, QUERY_TYPE, xxhashHex("")), Tuple1.of(fid), 2, getExpiry(fid)));

    // Item x Query
    for (long f : ImmutableList.of(
          FeatureId.itemQueryCount(AggMetric.COUNT_NAVIGATE, 1, "h"),
          FeatureId.itemQueryCount(AggMetric.COUNT_NAVIGATE, 1, "d"),
          FeatureId.itemQueryCount(AggMetric.COUNT_NAVIGATE, 7, "d"),
          FeatureId.itemQueryCount(AggMetric.COUNT_NAVIGATE, 30, "d"))) {
      assertThat(CollectingSink.output).containsAtLeast(
          RedisSink.hset(Tuple4.of(0, "NAVIGATE @ 600001", QUERY_TYPE, xxhashHex("query two")), Tuple1.of(f), 1, getExpiry(f)),
          RedisSink.hset(Tuple4.of(0, "NAVIGATE @ 1800000", QUERY_TYPE, xxhashHex("query two")), Tuple1.of(f), 1, getExpiry(f)))
        .inOrder();
      assertThat(CollectingSink.output).containsNoneOf(
          RedisSink.hset(Tuple4.of(0, "NAVIGATE @ 7200001", QUERY_TYPE, xxhashHex("query one")), Tuple1.of(f), 1, getExpiry(f)),
          RedisSink.hset(Tuple4.of(0, "NAVIGATE @ 259200000", QUERY_TYPE, xxhashHex("")), Tuple1.of(f), 1, getExpiry(f)),
          RedisSink.hset(Tuple4.of(0, "NAVIGATE @ 864000000", QUERY_TYPE, xxhashHex("")), Tuple1.of(f), 1, getExpiry(f)));
    }
  }

  @Test
  void userCounts() throws Exception {
    CounterJob job = createJob();
    setupCommonTestCase(job);
    waitForDone(env.execute("counter"));

    // User: todd
    for (Tuple3<Long, Long, Integer> test : ImmutableList.of(
          Tuple3.of(
            FeatureId.userCount(true, AggMetric.COUNT_NAVIGATE, 1, "h"),
            FeatureId.userCount(false, AggMetric.COUNT_NAVIGATE, 1, "h"),
            1),
          Tuple3.of(
            FeatureId.userCount(true, AggMetric.COUNT_NAVIGATE, 1, "d"),
            FeatureId.userCount(false, AggMetric.COUNT_NAVIGATE, 1, "d"),
            2),
          Tuple3.of(
            FeatureId.userCount(true, AggMetric.COUNT_NAVIGATE, 7, "d"),
            FeatureId.userCount(false, AggMetric.COUNT_NAVIGATE, 7, "d"),
            2),
          Tuple3.of(
            FeatureId.userCount(true, AggMetric.COUNT_NAVIGATE, 30, "d"),
            FeatureId.userCount(false, AggMetric.COUNT_NAVIGATE, 30, "d"),
            2))) {
      assertThat(CollectingSink.output).containsAtLeast(
          RedisSink.hset(Tuple3.of(0, USER_TYPE, "log_todd"), Tuple1.of(test.f0), test.f2, getExpiry(test.f1)),
          RedisSink.hset(Tuple3.of(0, USER_TYPE, "log_todd"), Tuple1.of(test.f0), 0, getExpiry(test.f0)))
        .inOrder();
      assertThat(CollectingSink.output).containsAtLeast(
          RedisSink.hset(Tuple3.of(0, USER_TYPE, "todd"), Tuple1.of(test.f1), test.f2, getExpiry(test.f1)),
          RedisSink.hset(Tuple3.of(0, USER_TYPE, "todd"), Tuple1.of(test.f1), 0, getExpiry(test.f1)))
        .inOrder();

      assertThat(CollectingSink.output).containsNoneOf(
          RedisSink.hset(Tuple3.of(0, USER_TYPE, "log_todd"), Tuple1.of(test.f0), test.f2 + 1, getExpiry(test.f1)),
          RedisSink.hset(Tuple3.of(0, USER_TYPE, "todd"), Tuple1.of(test.f1), test.f2 + 1, getExpiry(test.f1)));
    }

    // User: eve
    for (Tuple3<Long, Long, Integer> test : ImmutableList.of(
          Tuple3.of(
            FeatureId.userCount(true, AggMetric.COUNT_NAVIGATE, 1, "h"),
            FeatureId.userCount(false, AggMetric.COUNT_NAVIGATE, 1, "h"),
            1),
          Tuple3.of(
            FeatureId.userCount(true, AggMetric.COUNT_NAVIGATE, 1, "d"),
            FeatureId.userCount(false, AggMetric.COUNT_NAVIGATE, 1, "d"),
            1),
          Tuple3.of(
            FeatureId.userCount(true, AggMetric.COUNT_NAVIGATE, 7, "d"),
            FeatureId.userCount(false, AggMetric.COUNT_NAVIGATE, 7, "d"),
            2),
          Tuple3.of(
            FeatureId.userCount(true, AggMetric.COUNT_NAVIGATE, 30, "d"),
            FeatureId.userCount(false, AggMetric.COUNT_NAVIGATE, 30, "d"),
            3))) {
      assertThat(CollectingSink.output).containsAtLeast(
          RedisSink.hset(Tuple3.of(0, USER_TYPE, "log_eve"), Tuple1.of(test.f0), test.f2, getExpiry(test.f0)),
          RedisSink.hset(Tuple3.of(0, USER_TYPE, "log_eve"), Tuple1.of(test.f0), 0, getExpiry(test.f0)))
        .inOrder();
      assertThat(CollectingSink.output).containsAtLeast(
          RedisSink.hset(Tuple3.of(0, USER_TYPE, "eve"), Tuple1.of(test.f1), test.f2, getExpiry(test.f0)),
          RedisSink.hset(Tuple3.of(0, USER_TYPE, "eve"), Tuple1.of(test.f1), 0, getExpiry(test.f1)))
        .inOrder();

      assertThat(CollectingSink.output).containsNoneOf(
          RedisSink.hset(Tuple3.of(0, USER_TYPE, "log_eve"), Tuple1.of(test.f0), test.f2 + 1, getExpiry(test.f0)),
          RedisSink.hset(Tuple3.of(0, USER_TYPE, "eve"), Tuple1.of(test.f1), test.f2 + 1, getExpiry(test.f0)));
    }
  }

  @ParameterizedTest
  @EnumSource(names = {"USER_ITEM_COUNT", "USER_ITEM_HOURS_AGO", "LOG_USER_ITEM_COUNT", "LOG_USER_ITEM_HOURS_AGO"})
  void logUserItemLastTime(CountType countType) throws Exception {
    CounterJob job = createJob();
    setupCommonTestCase(job);
    waitForDone(env.execute("counter"));

    boolean isLogUser = EnumSet.of(CountType.LOG_USER_ITEM_COUNT, CountType.LOG_USER_ITEM_HOURS_AGO).contains(countType);
    String toddUid = isLogUser ? "log_todd" : "todd";
    String eveUid = isLogUser ? "log_eve" : "eve";

    boolean isTimestamp = EnumSet.of(CountType.USER_ITEM_HOURS_AGO, CountType.LOG_USER_ITEM_HOURS_AGO).contains(countType);
    long fid = isTimestamp
        ? FeatureId.lastUserContentTimestamp(isLogUser, AggMetric.COUNT_NAVIGATE)
        : FeatureId.lastUserContentCount(isLogUser, AggMetric.COUNT_NAVIGATE);

    // Rather than test all the redis commands, just check that certain values were set.
    assertThat(CollectingSink.output).containsAtLeast(
        RedisSink.hset(Tuple4.of(0, USER_TYPE, toddUid, "NAVIGATE @ 600001"), Tuple1.of(fid), Tuple1.of(isTimestamp ? 600001 : 1), getExpiry(fid, 1)),
        RedisSink.hset(Tuple4.of(0, USER_TYPE, toddUid, "NAVIGATE @ 7200001"), Tuple1.of(fid), Tuple1.of(isTimestamp ? 7200001 : 1), getExpiry(fid, 1)),
        RedisSink.hset(Tuple4.of(0, USER_TYPE, toddUid, "NAVIGATE @ 600001"), Tuple1.of(fid), Tuple1.of(isTimestamp ? 600001 : 0), getExpiry(fid, 0)),
        RedisSink.hset(Tuple4.of(0, USER_TYPE, toddUid, "NAVIGATE @ 7200001"), Tuple1.of(fid), Tuple1.of(isTimestamp ? 7200001 : 0), getExpiry(fid, 0)))
      .inOrder();
    assertThat(CollectingSink.output).containsAtLeast(
        RedisSink.hset(Tuple4.of(0, USER_TYPE, eveUid, "NAVIGATE @ 1800000"), Tuple1.of(fid), Tuple1.of(isTimestamp ? 1800000 : 1), getExpiry(fid, 1)),
        RedisSink.hset(Tuple4.of(0, USER_TYPE, eveUid, "NAVIGATE @ 259200000"), Tuple1.of(fid), Tuple1.of(isTimestamp ? 259200000 : 1), getExpiry(fid, 1)),
        RedisSink.hset(Tuple4.of(0, USER_TYPE, eveUid, "NAVIGATE @ 864000000"), Tuple1.of(fid), Tuple1.of(isTimestamp ? 864000000 : 1), getExpiry(fid, 1)),
        RedisSink.hset(Tuple4.of(0, USER_TYPE, eveUid, "NAVIGATE @ 1800000"), Tuple1.of(fid), Tuple1.of(isTimestamp ? 1800000 : 0), getExpiry(fid, 0)),
        RedisSink.hset(Tuple4.of(0, USER_TYPE, eveUid, "NAVIGATE @ 259200000"), Tuple1.of(fid), Tuple1.of(isTimestamp ? 259200000 : 0), getExpiry(fid, 0)),
        RedisSink.hset(Tuple4.of(0, USER_TYPE, eveUid, "NAVIGATE @ 864000000"), Tuple1.of(fid), Tuple1.of(isTimestamp ? 864000000 : 0), getExpiry(fid, 0)))
      .inOrder();
  }

  @ParameterizedTest
  @EnumSource(names = {"USER_QUERY_COUNT", "USER_QUERY_HOURS_AGO", "LOG_USER_QUERY_COUNT", "LOG_USER_QUERY_HOURS_AGO"})
  void logUserQueryLastTime(CountType countType) throws Exception {
    CounterJob job = createJob();
    setupCommonTestCase(job);
    waitForDone(env.execute("counter"));

    boolean isLogUser = EnumSet.of(CountType.LOG_USER_ITEM_COUNT, CountType.LOG_USER_ITEM_HOURS_AGO).contains(countType);
    String toddUid = isLogUser ? "log_todd" : "todd";
    String eveUid = isLogUser ? "log_eve" : "eve";

    boolean isTimestamp = EnumSet.of(CountType.USER_ITEM_HOURS_AGO, CountType.LOG_USER_ITEM_HOURS_AGO).contains(countType);
    long fid = isTimestamp
        ? FeatureId.lastUserQueryTimestamp(isLogUser, AggMetric.COUNT_NAVIGATE)
        : FeatureId.lastUserQueryCount(isLogUser, AggMetric.COUNT_NAVIGATE);

    // Rather than test all the redis commands, just check that certain values were set.
    assertThat(CollectingSink.output).containsAtLeast(
        RedisSink.hset(Tuple5.of(0, USER_TYPE, toddUid, QUERY_TYPE, xxhashHex("query two")), Tuple1.of(fid), Tuple1.of(isTimestamp ? 600001 : 1), getExpiry(fid, 1)),
        RedisSink.hset(Tuple5.of(0, USER_TYPE, toddUid, QUERY_TYPE, xxhashHex("query two")), Tuple1.of(fid), Tuple1.of(isTimestamp ? 600001 : 0), getExpiry(fid, 0)))
      .inOrder();
    assertThat(CollectingSink.output).containsAtLeast(
        RedisSink.hset(Tuple5.of(0, USER_TYPE, eveUid, QUERY_TYPE, xxhashHex("query two")), Tuple1.of(fid), Tuple1.of(isTimestamp ? 1800000 : 1), getExpiry(fid, 1)),
        RedisSink.hset(Tuple5.of(0, USER_TYPE, eveUid, QUERY_TYPE, xxhashHex("query two")), Tuple1.of(fid), Tuple1.of(isTimestamp ? 1800000 : 0), getExpiry(fid, 0)))
      .inOrder();
    assertThat(CollectingSink.output).containsNoneOf(
        RedisSink.hset(Tuple5.of(0, USER_TYPE, toddUid, QUERY_TYPE, xxhashHex("query one")), Tuple1.of(fid), Tuple1.of(isTimestamp ? 7200001 : 1), getExpiry(fid, 1)),
        RedisSink.hset(Tuple5.of(0, USER_TYPE, eveUid, QUERY_TYPE, xxhashHex("")), Tuple1.of(fid), Tuple1.of(isTimestamp ? 259200000 : 1), getExpiry(fid, 1)),
        RedisSink.hset(Tuple5.of(0, USER_TYPE, eveUid, QUERY_TYPE, xxhashHex("")), Tuple1.of(fid), Tuple1.of(isTimestamp ? 864000000 : 1), getExpiry(fid, 1)),
        RedisSink.hset(Tuple5.of(0, USER_TYPE, toddUid, QUERY_TYPE, xxhashHex("query one")), Tuple1.of(fid), Tuple1.of(isTimestamp ? 7200001 : 0), getExpiry(fid, 0)),
        RedisSink.hset(Tuple5.of(0, USER_TYPE, eveUid, QUERY_TYPE, xxhashHex("")), Tuple1.of(fid), Tuple1.of(isTimestamp ? 864000000 : 0), getExpiry(fid, 0)));
  }

  private static long getExpiry(long fid) {
    CountWindow window = CountWindow.forNumber(Math.toIntExact(fid & CountFeatureMask.WINDOW.getNumber()));
    switch (window) {
        case DAY_30:
            return SlidingDailyCounter.EXPIRE_TTL_SECONDS;
        case DAY_90:
            throw new IllegalArgumentException("use the getExpiry(fid, count) for last user event counts");
        default:
            return 0;
    }
  }

  private static long getExpiry(long fid, long count) {
    CountWindow window = CountWindow.forNumber(Math.toIntExact(fid & CountFeatureMask.WINDOW.getNumber()));
    switch (window) {
        case DAY_30:
            throw new IllegalArgumentException("use the getExpiry(fid) for regular counts");
        case DAY_90:
            return count == 0 ? LastTimeAndCount.EXPIRE_TTL_SECONDS : LastTimeAndCount.KEEP_TTL_SECONDS;
        default:
            return 0;
    }
  }

  private static JoinedEvent joinedEvent(Action.Builder action, String query) {
    JoinedEvent.Builder builder = FlatUtil.setFlatAction(JoinedEvent.newBuilder().setTiming(action.getTiming()), action.build(), (tag, error) -> {});

    boolean isEven = action.getTiming().getEventApiTimestamp() % 2 == 0;

    builder.getUserBuilder().getUserInfoBuilder()
      .setUserId(isEven ? "eve" : "todd")
      .setLogUserId(isEven ? "log_eve" : "log_todd");
    builder.getRequestBuilder().getDeviceBuilder().getBrowserBuilder()
      .setUserAgent(isEven ? "ccc/980 CFNetwork/1240.0.4 Darwin/20.6.0" : "okhttp/3.14.9");
    if (query != "") {
      builder.getRequestBuilder().setSearchQuery(query);
    }

    LOGGER.trace(builder);
    return builder.build();
  }

  private static Action.Builder createAction(ActionType actionType, long eventTime) {
    Action.Builder builder = Action.newBuilder()
      .setActionType(actionType)
      .setContentId(String.format("%s @ %s", actionType, eventTime));
    builder.getTimingBuilder().setEventApiTimestamp(eventTime);
    return builder;
  }

  /** SinkFunction used to collect stream output for test validation. */
  static class CollectingSink implements SinkFunction<RedisSink.Command> {
    // TODO: this doesn't play nice with concurrency (multiple concurrent test runs) and difficult
    // to reuse.  As a consequence, we tagged the test target in the bazel target as "exclusive".
    // TODO: look into DataStream.executeAndCollect as a replacement
    public static final List<RedisSink.Command> output = Collections.synchronizedList(new ArrayList<>());
    public long currentTimestamp = Long.MIN_VALUE;

    public void invoke(RedisSink.Command value, Context ctx) {
      if (ctx != null && ctx.timestamp() != null) {
        currentTimestamp = ctx.timestamp();
      }
      LOGGER.trace("address: output({}) this({}) timestamp: {} watermark= {} value: {}",
          System.identityHashCode(CollectingSink.output), System.identityHashCode(this), currentTimestamp, ctx.currentWatermark(), value);
      CollectingSink.output.add(value);
      LOGGER.debug("CollectingSink.output.size: {}", CollectingSink.output.size());
    }
  }
}
