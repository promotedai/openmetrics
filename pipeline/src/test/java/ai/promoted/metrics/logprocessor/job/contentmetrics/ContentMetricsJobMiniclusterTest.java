package ai.promoted.metrics.logprocessor.job.contentmetrics;

import static ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory.createLogRequestOptionsBuilder;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.IncrementingUUIDSupplier;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSourceProvider;
import ai.promoted.metrics.logprocessor.common.job.ValidatedDataSourceProvider;
import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobMiniclusterTest;
import ai.promoted.metrics.logprocessor.common.table.Tables;
import ai.promoted.metrics.logprocessor.common.testing.MiniClusterExtension;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.View;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Warning: this test runs the stream jobs as batch. Flink stream minicluster tests don't
 * automatically checkpoint at the end. TODO - add link to thread about this.
 */
@ExtendWith(MiniClusterExtension.class)
public class ContentMetricsJobMiniclusterTest extends BaseJobMiniclusterTest<ContentMetricsJob> {

  private static long toEpochMilli(LocalDateTime time) {
    return time.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  private static void assertRowsUnorderedEqual(List<Row> expected, List<Row> actual) {
    Set<Row> expectedSet = expected.stream().collect(Collectors.toSet());
    Set<Row> actualSet = actual.stream().collect(Collectors.toSet());

    Sets.SetView<Row> expectedDiffs = Sets.difference(expectedSet, actualSet);
    Sets.SetView<Row> actualDiffs = Sets.difference(actualSet, expectedSet);

    assertAll(
        "record",
        Streams.concat(
            expectedDiffs.stream()
                .map(
                    (row) ->
                        () -> {
                          fail("Did not find expected row=" + row);
                        }),
            actualDiffs.stream()
                .map(
                    (row) ->
                        () -> {
                          fail("Found unexpected row=" + row);
                        })));
  }

  private static JoinedImpression createTestJoinedImpression(
      String impressionId, LocalDateTime time, String contentId, long position) {
    return JoinedImpression.newBuilder()
        .setIds(
            JoinedIdentifiers.newBuilder().setPlatformId(1L).setImpressionId(impressionId).build())
        .setTiming(Timing.newBuilder().setEventApiTimestamp(toEpochMilli(time)))
        .setResponseInsertion(Insertion.newBuilder().setContentId(contentId).setPosition(position))
        .build();
  }

  private static boolean isSuccessFile(File file) {
    return file.getName().equals("_SUCCESS");
  }

  private static Set<String> filesInDir(File file) {
    return nullableSetOf(file.list());
  }

  private static Set<String> nullableSetOf(@Nullable String[] array) {
    return ImmutableSet.copyOf(nullToEmptyArray(array));
  }

  private static String[] nullToEmptyArray(String[] array) {
    return array != null ? array : new String[0];
  }

  private ValidatedDataSourceProvider fakeValidatedDataSourceProvider;
  private FlatOutputKafkaSourceProvider fakeFlatOutputKafkaSourceProvider;

  @BeforeEach
  public void setUp() {
    super.setUp();
  }

  protected RuntimeExecutionMode getRuntimeMode() {
    return RuntimeExecutionMode.STREAMING;
  }

  @Override
  protected ContentMetricsJob createJob() {
    ContentMetricsJob job = new ContentMetricsJob();
    job.disableAutoGeneratedUIDs = false;
    job.jobLabel = "blue";
    job.s3.rootPath = tempDir.getAbsolutePath();
    // Checkpoint more frequently so we don't hit part files.
    job.checkpointInterval = Duration.ofSeconds(1);
    job.checkpointTimeout = Duration.ofSeconds(60);
    job.configureExecutionEnvironment(env, 1, 0);
    return job;
  }

  private void setupStreams(
      List<LogRequest> logRequests, List<JoinedImpression> joinedImpressions) {
    List<View> view = LogRequestFactory.pushDownToViews(logRequests);
    List<Impression> impression = LogRequestFactory.pushDownToImpressions(logRequests);
    List<Action> action = LogRequestFactory.pushDownToActions(logRequests);

    SingleOutputStreamOperator<View> viewStream =
        fromItems(env, "view", view, TypeInformation.of(View.class), View::getTiming);
    SingleOutputStreamOperator<Impression> impressionStream =
        fromItems(
            env,
            "impression",
            impression,
            TypeInformation.of(Impression.class),
            Impression::getTiming);
    SingleOutputStreamOperator<Action> actionStream =
        fromItems(env, "action", action, TypeInformation.of(Action.class), Action::getTiming);
    SingleOutputStreamOperator<JoinedImpression> joinedImpressionStream =
        fromItems(
            env,
            "joined-impression",
            joinedImpressions,
            TypeInformation.of(JoinedImpression.class),
            JoinedImpression::getTiming);
    // Not used by the job operators but the MetricsTableCatalog code sets it up.
    SingleOutputStreamOperator<AttributedAction> attributedActionStream =
        fromItems(
            env,
            "attributed-action",
            ImmutableList.of(),
            TypeInformation.of(AttributedAction.class),
            getTestWatermarkStrategy(ignored -> 0L));
    fakeFlatOutputKafkaSourceProvider =
        new FlatOutputKafkaSourceProvider() {
          @Override
          public SingleOutputStreamOperator<JoinedImpression> getJoinedImpressionSource(
              StreamExecutionEnvironment env, String consumerGroupId) {
            return joinedImpressionStream;
          }

          @Override
          public SingleOutputStreamOperator<AttributedAction> getAttributedActionSource(
              StreamExecutionEnvironment env, String consumerGroupId) {
            return attributedActionStream;
          }
        };
    fakeValidatedDataSourceProvider =
        new ValidatedDataSourceProvider() {
          @Override
          public SingleOutputStreamOperator<View> getViewSource(
              StreamExecutionEnvironment env, String consumerGroupId) {
            return viewStream;
          }

          @Override
          public SingleOutputStreamOperator<Impression> getImpressionSource(
              StreamExecutionEnvironment env, String consumerGroupId) {
            return impressionStream;
          }

          @Override
          public SingleOutputStreamOperator<Action> getActionSource(
              StreamExecutionEnvironment env, String consumerGroupId) {
            return actionStream;
          }
        };
  }

  @Test
  void testSimple() throws Exception {
    ContentMetricsJob job = createJob();
    job.outputParquet = true;

    LocalDateTime time = LocalDateTime.of(2022, 10, 1, 22, 49, 11);
    List<LogRequest> inputLogRequests =
        LogRequestFactory.createLogRequests(
            createLogRequestOptionsBuilder(
                    toEpochMilli(time), LogRequestFactory.DetailLevel.PARTIAL, 1)
                .setWriteProductViews(true)
                .setNavigateCheckoutRate(1.0f)
                .setNavigateAddToCartRate(1.0f)
                .setCheckoutPurchaseRate(1.0f));
    List<JoinedImpression> joinedImpressions =
        ImmutableList.of(createTestJoinedImpression("imp1", time, "i-1-1", 5));
    setupStreams(inputLogRequests, joinedImpressions);

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    Tables.createCatalogAndDatabase(tableEnv);

    waitForDone(
        job.createOperators(
            env, tableEnv, fakeValidatedDataSourceProvider, fakeFlatOutputKafkaSourceProvider));
    TableResult hourlyContentMetrics =
        tableEnv.executeSql("SELECT * FROM `hourly_content_metrics_view`");

    assertRowsUnorderedEqual(
        ImmutableList.of(
            Row.of("2022-10-01", "22", 1L, "i-1-1", 1L, 1L, 1L, 5L, 1L, 1L, 2L, 2L, 5000000L)),
        ImmutableList.copyOf(hourlyContentMetrics.collect()));

    // Asserts that some files.  We cannot assert the file contents though because Flink doesn't
    // output all of the rows to
    // the File sink before finishing.
    assertEquals(
        ImmutableSet.of("hour=22"),
        filesInDir(new File(tempDir, "blue/etl/hourly_content_metrics/dt=2022-10-01")));
    // TODO - Fix Flink bug that provides incomplete sinks in Minicluster tests.
  }

  // More data and duplicates.
  @Test
  void testComplex() throws Exception {
    ContentMetricsJob job = createJob();
    job.outputParquet = true;

    LocalDateTime time1 = LocalDateTime.of(2022, 10, 1, 22, 49, 11);
    List<LogRequest> inputLogRequests =
        ImmutableList.<LogRequest>builder()
            .addAll(
                LogRequestFactory.createLogRequests(
                    createLogRequestOptionsBuilder(
                            toEpochMilli(time1), LogRequestFactory.DetailLevel.PARTIAL, 3)
                        .setWriteProductViews(true)
                        .setNavigateCheckoutRate(0.5f)
                        .setNavigateAddToCartRate(0.5f)
                        .setCheckoutPurchaseRate(0.75f)))
            .addAll(
                LogRequestFactory.createLogRequests(
                    createLogRequestOptionsBuilder(
                            toEpochMilli(LocalDateTime.of(2022, 10, 1, 23, 49, 11)),
                            LogRequestFactory.DetailLevel.PARTIAL,
                            2)
                        // Use different UUIDs so the events do not get de-duplicated.
                        .setCohortMembershipUuidSupplier(
                            new IncrementingUUIDSupplier("CCCCCCCD-CCCC-CCCC-0000-000000000000"))
                        .setSessionUuidSupplier(
                            new IncrementingUUIDSupplier("11111112-1111-1111-0000-000000000000"))
                        .setViewUuidSupplier(
                            new IncrementingUUIDSupplier("22222223-2222-2222-0000-000000000000"))
                        .setAutoViewUuidSupplier(
                            new IncrementingUUIDSupplier("77777778-7777-7777-0000-000000000000"))
                        .setRequestUuidSupplier(
                            new IncrementingUUIDSupplier("33333334-3333-3333-0000-000000000000"))
                        .setResponseInsertionUuidSupplier(
                            new IncrementingUUIDSupplier("44444445-4444-4444-0000-000000000000"))
                        .setImpressionUuidSupplier(
                            new IncrementingUUIDSupplier("55555556-5555-5555-0000-000000000000"))
                        .setActionUuidSupplier(
                            new IncrementingUUIDSupplier("66666667-6666-6666-0000-000000000000"))
                        .setWriteProductViews(true)
                        .setNavigateCheckoutRate(0.5f)
                        .setNavigateAddToCartRate(0.5f)
                        .setCheckoutPurchaseRate(0.75f)))
            .addAll(
                LogRequestFactory.createLogRequests(
                    createLogRequestOptionsBuilder(
                            toEpochMilli(LocalDateTime.of(2022, 10, 2, 00, 49, 11)),
                            LogRequestFactory.DetailLevel.PARTIAL,
                            1)
                        // Use different UUIDs so the events do not get de-duplicated.
                        .setCohortMembershipUuidSupplier(
                            new IncrementingUUIDSupplier("CCCCCCCE-CCCC-CCCC-0000-000000000000"))
                        .setSessionUuidSupplier(
                            new IncrementingUUIDSupplier("11111113-1111-1111-0000-000000000000"))
                        .setViewUuidSupplier(
                            new IncrementingUUIDSupplier("22222224-2222-2222-0000-000000000000"))
                        .setAutoViewUuidSupplier(
                            new IncrementingUUIDSupplier("77777779-7777-7777-0000-000000000000"))
                        .setRequestUuidSupplier(
                            new IncrementingUUIDSupplier("33333335-3333-3333-0000-000000000000"))
                        .setResponseInsertionUuidSupplier(
                            new IncrementingUUIDSupplier("44444446-4444-4444-0000-000000000000"))
                        .setImpressionUuidSupplier(
                            new IncrementingUUIDSupplier("55555557-5555-5555-0000-000000000000"))
                        .setActionUuidSupplier(
                            new IncrementingUUIDSupplier("66666668-6666-6666-0000-000000000000"))
                        .setWriteProductViews(true)
                        .setNavigateCheckoutRate(0.5f)
                        .setNavigateAddToCartRate(0.5f)
                        .setCheckoutPurchaseRate(0.75f)))
            .build();
    List<JoinedImpression> joinedImpressions =
        ImmutableList.of(
            createTestJoinedImpression("imp1", time1, "i-1-1", 5),
            createTestJoinedImpression("imp2", time1, "i-1-1", 3));
    setupStreams(inputLogRequests, joinedImpressions);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    Tables.createCatalogAndDatabase(tableEnv);

    waitForDone(
        job.createOperators(
            env, tableEnv, fakeValidatedDataSourceProvider, fakeFlatOutputKafkaSourceProvider));

    TableResult hourlyContentMetrics =
        tableEnv.executeSql("SELECT * FROM `hourly_content_metrics_view`");
    // TODO - debug why the other contentIds don't have navigates.
    // TODO - debug why it's just these contentIds.
    assertRowsUnorderedEqual(
        ImmutableList.of(
            Row.of("2022-10-01", "22", 1L, "i-1-1", 7L, 81L, 2L, 8L, 7L, 5L, 17L, 17L, 60000000L),
            Row.of("2022-10-01", "22", 1L, "i-1-10", 0L, 81L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
            Row.of("2022-10-01", "22", 1L, "i-1-100", 0L, 81L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
            Row.of("2022-10-01", "23", 1L, "i-1-1", 0L, 16L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
            Row.of("2022-10-01", "23", 1L, "i-1-10", 0L, 16L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
            Row.of("2022-10-02", "00", 1L, "i-1-1", 1L, 1L, 0L, 0L, 1L, 0L, 0L, 0L, 0L)),
        ImmutableList.copyOf(hourlyContentMetrics.collect()));

    TableResult dailyContentMetrics =
        tableEnv.executeSql("SELECT * FROM `daily_content_metrics_view`");
    assertRowsUnorderedEqual(
        ImmutableList.of(
            Row.of("2022-10-01", 1L, "i-1-1", 7L, 97L, 2L, 8L, 7L, 5L, 17L, 17L, 60000000L),
            Row.of("2022-10-01", 1L, "i-1-10", 0L, 97L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
            Row.of("2022-10-01", 1L, "i-1-100", 0L, 81L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
            Row.of("2022-10-02", 1L, "i-1-1", 1L, 1L, 0L, 0L, 1L, 0L, 0L, 0L, 0L)),
        ImmutableList.copyOf(dailyContentMetrics.collect()));

    // Asserts that some files.  We cannot assert the file contents though because Flink doesn't
    // output all of the rows to
    // the File sink before finishing.
    assertEquals(
        ImmutableSet.of("hour=22", "hour=23"),
        filesInDir(new File(tempDir, "blue/etl/hourly_content_metrics/dt=2022-10-01")));
    assertEquals(
        ImmutableSet.of("hour=00"),
        filesInDir(new File(tempDir, "blue/etl/hourly_content_metrics/dt=2022-10-02")));
    // TODO - Fix Flink bug that provides incomplete sinks in Minicluster tests.
  }

  @Test
  void testCsv() throws Exception {
    ContentMetricsJob job = createJob();
    job.outputParquet = true;
    job.outputCsv = true;

    LocalDateTime time = LocalDateTime.of(2022, 10, 1, 22, 49, 11);
    List<LogRequest> inputLogRequests =
        LogRequestFactory.createLogRequests(
            createLogRequestOptionsBuilder(
                    toEpochMilli(time), LogRequestFactory.DetailLevel.PARTIAL, 2)
                .setWriteProductViews(true)
                .setNavigateCheckoutRate(1.0f)
                .setNavigateAddToCartRate(1.0f)
                .setCheckoutPurchaseRate(1.0f));
    List<JoinedImpression> joinedImpressions =
        ImmutableList.of(createTestJoinedImpression("imp1", time, "i-1-1", 5));
    setupStreams(inputLogRequests, joinedImpressions);

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    Tables.createCatalogAndDatabase(tableEnv);

    waitForDone(
        job.createOperators(
            env, tableEnv, fakeValidatedDataSourceProvider, fakeFlatOutputKafkaSourceProvider));
    TableResult hourlyContentMetrics =
        tableEnv.executeSql("SELECT * FROM `hourly_content_metrics_view`");

    assertRowsUnorderedEqual(
        ImmutableList.of(
            Row.of("2022-10-01", "22", 1L, "i-1-1", 2L, 16L, 1L, 5L, 2L, 2L, 4L, 4L, 10000000L),
            Row.of("2022-10-01", "22", 1L, "i-1-10", 0L, 16L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)),
        ImmutableList.copyOf(hourlyContentMetrics.collect()));

    // Asserts that some files.  We cannot assert the file contents though because Flink doesn't
    // output all of the rows to
    // the File sink before finishing.
    File[] filesArray =
        new File(tempDir, "blue/etl/daily_content_metrics_csv/dt=2022-10-01/").listFiles();
    List<File> files =
        Arrays.stream(filesArray)
            .filter(Predicate.not(ContentMetricsJobMiniclusterTest::isSuccessFile))
            .collect(Collectors.toList());
    assertEquals(1, files.size());
    String filePath = files.get(0).getPath();
    assertTrue(
        filePath.endsWith(".csv"), () -> "File should end with a `.csv` suffix, " + filePath);

    List<String> actualLines = Files.readAllLines(Paths.get(files.get(0).getPath()));
    assertEquals(3, actualLines.size());
    assertEquals(
        "date,platform_id,content_id,view_count,impression_count,joined_impression_count,\"joined_impression_position_sum\",navigate_count,add_to_cart_count,checkout_count,purchase_count,gmv_usd_micros",
        actualLines.get(0));
    assertEquals("2022-10-01,1,i-1-1,2,16,1,5,2,2,4,4,10000000", actualLines.get(1));
    assertEquals("2022-10-01,1,i-1-10,0,16,0,0,0,0,0,0,0", actualLines.get(2));

    // TODO - Fix Flink bug that provides incomplete sinks in Minicluster tests.
  }

  @Test
  void testCumulatedJSON() throws Exception {
    ContentMetricsJob job = createJob();
    job.outputParquet = true;
    job.outputCumulatedFiles = true;

    LocalDateTime time = LocalDateTime.of(2022, 10, 1, 22, 49, 11);
    List<LogRequest> inputLogRequests =
        LogRequestFactory.createLogRequests(
            createLogRequestOptionsBuilder(
                    toEpochMilli(time), LogRequestFactory.DetailLevel.PARTIAL, 1)
                .setWriteProductViews(true)
                .setNavigateCheckoutRate(1.0f)
                .setNavigateAddToCartRate(1.0f)
                .setCheckoutPurchaseRate(1.0f));
    List<JoinedImpression> joinedImpressions =
        ImmutableList.of(createTestJoinedImpression("imp1", time, "i-1-1", 5));
    setupStreams(inputLogRequests, joinedImpressions);

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    Tables.createCatalogAndDatabase(tableEnv);

    waitForDone(
        job.createOperators(
            env, tableEnv, fakeValidatedDataSourceProvider, fakeFlatOutputKafkaSourceProvider));
    TableResult cumulatedContentMetrics =
        tableEnv.executeSql("SELECT * FROM `cumulated_content_metrics_view`");

    LocalDateTime windowStart = LocalDateTime.of(2022, 10, 1, 0, 0, 0);
    LocalDateTime windowEnd = LocalDateTime.of(2022, 10, 2, 0, 0, 0);
    assertRowsUnorderedEqual(
        ImmutableList.of(
            Row.of(windowStart, windowEnd, 1L, "i-1-1", 1L, 1L, 1L, 5L, 1L, 1L, 2L, 2L, 5000000L)),
        ImmutableList.copyOf(cumulatedContentMetrics.collect()));
    // TODO - Fix Flink bug that provides incomplete sinks in Minicluster tests.

    File[] filesArray =
        new File(tempDir, "blue/etl/cumulated_content_metrics/dt=2022-10-01").listFiles();
    List<File> files =
        Arrays.stream(filesArray)
            .filter(Predicate.not(ContentMetricsJobMiniclusterTest::isSuccessFile))
            .collect(Collectors.toList());
    assertEquals(1, files.size());
    List<String> actualLines = Files.readAllLines(Paths.get(files.get(0).getPath()));

    assertEquals(1, actualLines.size());
    assertEquals(
        "{\"header\":{\"version\":\"1.0\",\"sourceId\":\"Metric\",\"id\":\"2022-10-01@1@i-1-1\","
            + "\"correlationId\":\"TODO\",\"platformId\":1,\"messageType\":\"blue.content-metrics\","
            + "\"eventDateTimestamp\":\"2022-10-02T00:00:00Z\"},"
            + "\"body\":{\"contentId\":\"i-1-1\",\"metrics\":{\"views\":1,\"impressions\":1,"
            + "\"joinedImpressions\":1,\"joinedImpressionPositionSum\":5,\"navigates\":1,\"addToCarts\":1,"
            + "\"checkouts\":2,\"purchases\":2,\"gmvUsd\":5000000}}}",
        actualLines.get(0));
  }

  private void waitForDone(Iterable<TableResult> tableResults)
      throws InterruptedException, ExecutionException {
    for (TableResult tableResult : tableResults) {
      waitForDone(tableResult);
    }
  }

  private void waitForDone(TableResult tableResult)
      throws InterruptedException, ExecutionException {
    waitForDone(tableResult.getJobClient().get().getJobID());
  }
}
