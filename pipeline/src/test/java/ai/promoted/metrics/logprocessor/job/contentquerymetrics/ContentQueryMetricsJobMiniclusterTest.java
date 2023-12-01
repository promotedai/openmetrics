package ai.promoted.metrics.logprocessor.job.contentquerymetrics;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.promoted.metrics.logprocessor.common.functions.inferred.AttributionModel;
import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobMiniclusterTest;
import ai.promoted.metrics.logprocessor.common.table.Tables;
import ai.promoted.metrics.logprocessor.common.testing.MiniClusterExtension;
import ai.promoted.proto.common.CurrencyCode;
import ai.promoted.proto.common.Money;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.Attribution;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.Touchpoint;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
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
public class ContentQueryMetricsJobMiniclusterTest
    extends BaseJobMiniclusterTest<ContentQueryMetricsJob> {

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
      String impressionId,
      LocalDateTime time,
      String contentId,
      String searchQuery,
      long position) {
    return JoinedImpression.newBuilder()
        .setIds(
            JoinedIdentifiers.newBuilder().setPlatformId(1L).setImpressionId(impressionId).build())
        .setTiming(Timing.newBuilder().setEventApiTimestamp(toEpochMilli(time)))
        .setRequest(Request.newBuilder().setSearchQuery(searchQuery))
        .setResponseInsertion(Insertion.newBuilder().setContentId(contentId).setPosition(position))
        .build();
  }

  private static AttributedAction createTestAttributedAction(
      String actionId,
      LocalDateTime time,
      String contentId,
      String searchQuery,
      ActionType actionType,
      Optional<CartContent> cartContent) {
    Action.Builder actionBuilder =
        Action.newBuilder()
            .setPlatformId(1L)
            .setActionId(actionId)
            .setActionType(actionType)
            .setTiming(Timing.newBuilder().setEventApiTimestamp(toEpochMilli(time)))
            .setContentId(contentId);
    if (cartContent.isPresent()) {
      actionBuilder.setSingleCartContent(cartContent.get());
    }
    return AttributedAction.newBuilder()
        .setAction(actionBuilder)
        .setAttribution(
            Attribution.newBuilder().setModelId(AttributionModel.LATEST.id).setCreditMillis(1000))
        .setTouchpoint(
            Touchpoint.newBuilder()
                .setJoinedImpression(
                    createTestJoinedImpression(
                        actionId + "-impressionId", time, contentId, searchQuery, 0L)))
        .build();
  }

  private static CartContent createTestCartContent(long quantity, long amountMicros) {
    return CartContent.newBuilder()
        .setQuantity(quantity)
        .setPricePerUnit(
            Money.newBuilder().setAmountMicros(amountMicros).setCurrencyCode(CurrencyCode.USD))
        .build();
  }

  private static boolean isSuccessFile(File file) {
    return file.getName().equals("_SUCCESS");
  }

  private static Set<String> filesInDir(File file) {
    return nullableSetOf(file.list());
  }

  private static Set<String> nullableSetOf(String[] array) {
    return ImmutableSet.copyOf(nullToEmptyArray(array));
  }

  private static String[] nullToEmptyArray(String[] array) {
    return array != null ? array : new String[0];
  }

  DataStream<JoinedImpression> joinedImpressions;
  DataStream<AttributedAction> attributedActions;

  @Override
  @BeforeEach
  public void setUp() {
    super.setUp();
    joinedImpressions =
        fromItems(
            env,
            "joined-impression",
            createTestJoinedImpressions(),
            TypeInformation.of(JoinedImpression.class),
            JoinedImpression::getTiming);
    attributedActions =
        fromItems(
            env,
            "attributed-action",
            createTestAttributedActions(),
            TypeInformation.of(AttributedAction.class),
            (AttributedAction attributedAction) -> attributedAction.getAction().getTiming());
  }

  protected RuntimeExecutionMode getRuntimeMode() {
    return RuntimeExecutionMode.STREAMING;
  }

  @Override
  protected ContentQueryMetricsJob createJob() {
    ContentQueryMetricsJob job = new ContentQueryMetricsJob();
    job.disableAutoGeneratedUIDs = false;
    job.jobLabel = "blue";
    job.s3.rootPath = tempDir.getAbsolutePath();
    // Checkpoint more frequently so we don't hit part files.
    job.checkpointInterval = Duration.ofSeconds(1);
    job.checkpointTimeout = Duration.ofSeconds(60);
    job.configureExecutionEnvironment(env, 1, 0);
    return job;
  }

  private List<JoinedImpression> createTestJoinedImpressions() {
    LocalDateTime time = LocalDateTime.of(2022, 10, 1, 22, 49, 11);
    return ImmutableList.of(
        createTestJoinedImpression("imp1", time, "i-1-1", "query1", 5),
        createTestJoinedImpression("imp2", time, "i-1-2", "query2", 2),
        createTestJoinedImpression("imp3", time, "i-1-2", "query2", 3),
        createTestJoinedImpression("imp4", time, "i-1-2", "query2", 5));
  }

  private List<AttributedAction> createTestAttributedActions() {
    LocalDateTime time = LocalDateTime.of(2022, 10, 1, 22, 49, 11);

    List<AttributedAction> attributedActions =
        ImmutableList.of(
            createTestAttributedAction(
                "act1", time, "i-1-1", "query1", ActionType.NAVIGATE, Optional.empty()),
            createTestAttributedAction(
                "act2", time, "i-1-2", "query2", ActionType.ADD_TO_CART, Optional.empty()),
            // The cart content on checkout should be ignored.
            createTestAttributedAction(
                "act3",
                time,
                "i-1-2",
                "query2",
                ActionType.CHECKOUT,
                Optional.of(createTestCartContent(2L, 1000000L))),
            createTestAttributedAction(
                "act4",
                time,
                "i-1-2",
                "query2",
                ActionType.PURCHASE,
                Optional.of(createTestCartContent(2L, 1000000L))),
            createTestAttributedAction(
                "act4",
                time,
                "i-1-3",
                "query2",
                ActionType.PURCHASE,
                Optional.of(createTestCartContent(1L, 1000000L))),
            createTestAttributedAction(
                "act5",
                time,
                "i-1-3",
                "query3",
                ActionType.PURCHASE,
                Optional.of(createTestCartContent(0L, 0L))));

    // Non-last Attribution Models currently get ignored.
    return addEvenAttributedActions(attributedActions);
  }

  private static List<AttributedAction> addEvenAttributedActions(
      List<AttributedAction> attributedActions) {
    return attributedActions.stream()
        .flatMap(
            action ->
                Stream.of(action, cloneAndSetAttributionModelId(action, AttributionModel.EVEN.id)))
        .collect(Collectors.toList());
  }

  private static AttributedAction cloneAndSetAttributionModelId(
      AttributedAction attributedAction, long attributionModelId) {
    AttributedAction.Builder builder = attributedAction.toBuilder();
    builder.getAttributionBuilder().setModelId(attributionModelId);
    return builder.build();
  }

  @Test
  void testDailyParquet() throws Exception {
    ContentQueryMetricsJob job = createJob();
    job.daily = true;
    job.outputParquet = true;
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    Tables.createCatalogAndDatabase(tableEnv);

    waitForDone(job.createOperators(tableEnv, joinedImpressions, attributedActions));
    TableResult dailyContentQueryMetrics =
        tableEnv.executeSql("SELECT * FROM `daily_content_query_joined_metrics_view`");

    assertRowsUnorderedEqual(
        ImmutableList.of(
            Row.of("2022-10-01", 1L, "i-1-1", "query1", 1L, 5L, 1L, 0L, 0L, 0L, 0L),
            // 2 quantity on purchase.
            Row.of("2022-10-01", 1L, "i-1-2", "query2", 3L, 10L, 0L, 1L, 2L, 2L, 2000000L),
            Row.of("2022-10-01", 1L, "i-1-3", "query2", 0L, 0L, 0L, 0L, 0L, 1L, 1000000L),
            // 0 quantity gets converted to 1 quantity.
            Row.of("2022-10-01", 1L, "i-1-3", "query3", 0L, 0L, 0L, 0L, 0L, 1L, 0L)),
        ImmutableList.copyOf(dailyContentQueryMetrics.collect()));

    // Asserts that some files.  We cannot assert the file contents though because Flink doesn't
    // output all of the rows to the File sink before finishing.
    assertEquals(
        ImmutableSet.of("dt=2022-10-01"),
        filesInDir(new File(tempDir, "blue/etl/daily_content_query_joined_metrics")));
    // TODO - Fix Flink bug that provides incomplete sinks in Minicluster tests.
  }

  @Test
  void testWeeklyParquet() throws Exception {
    ContentQueryMetricsJob job = createJob();
    job.weekly = true;
    job.outputParquet = true;
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    Tables.createCatalogAndDatabase(tableEnv);

    waitForDone(job.createOperators(tableEnv, joinedImpressions, attributedActions));
    TableResult weeklyContentQueryMetrics =
        tableEnv.executeSql("SELECT * FROM `weekly_content_query_joined_metrics_view`");

    assertRowsUnorderedEqual(
        ImmutableList.of(
            Row.of("2022-09-25", 1L, "i-1-1", "query1", 1L, 5L, 1L, 0L, 0L, 0L, 0L),
            // 2 quantity on purchase.
            Row.of("2022-09-25", 1L, "i-1-2", "query2", 3L, 10L, 0L, 1L, 2L, 2L, 2000000L),
            Row.of("2022-09-25", 1L, "i-1-3", "query2", 0L, 0L, 0L, 0L, 0L, 1L, 1000000L),
            // 0 quantity gets converted to 1 quantity.
            Row.of("2022-09-25", 1L, "i-1-3", "query3", 0L, 0L, 0L, 0L, 0L, 1L, 0L)),
        ImmutableList.copyOf(weeklyContentQueryMetrics.collect()));

    // Asserts that some files.  We cannot assert the file contents though because Flink doesn't
    // output all of the rows to the File sink before finishing.
    assertEquals(
        ImmutableSet.of("dt=2022-09-25"),
        filesInDir(new File(tempDir, "blue/etl/weekly_content_query_joined_metrics")));
    // TODO - Fix Flink bug that provides incomplete sinks in Minicluster tests.
  }

  @Test
  void testDailyCsv() throws Exception {
    ContentQueryMetricsJob job = createJob();
    job.daily = true;
    job.outputCsv = true;
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    Tables.createCatalogAndDatabase(tableEnv);

    waitForDone(job.createOperators(tableEnv, joinedImpressions, attributedActions));

    // Asserts that some files.  We cannot assert the file contents though because Flink doesn't
    // output all of the rows to
    // the File sink before finishing.
    File[] filesArray =
        new File(tempDir, "blue/etl/daily_content_query_joined_metrics_csv/dt=2022-10-01/")
            .listFiles();
    List<File> files =
        Arrays.stream(filesArray)
            .filter(Predicate.not(ContentQueryMetricsJobMiniclusterTest::isSuccessFile))
            .collect(Collectors.toList());
    assertEquals(1, files.size());
    String filePath = files.get(0).getPath();
    assertTrue(
        filePath.endsWith(".csv"), () -> "File should end with a `.csv` suffix, " + filePath);

    List<String> actualLines = Files.readAllLines(Paths.get(files.get(0).getPath()));
    assertEquals(5, actualLines.size());
    assertEquals(
        "date,platform_id,content_id,search_query,impression_count,impression_position_sum,navigate_count,add_to_cart_count,checkout_count,purchase_count,gmv_usd_micros",
        actualLines.get(0));
    assertEquals("2022-10-01,1,i-1-1,query1,1,5,1,0,0,0,0", actualLines.get(1));
    assertEquals("2022-10-01,1,i-1-2,query2,3,10,0,1,2,2,2000000", actualLines.get(2));
    assertEquals("2022-10-01,1,i-1-3,query3,0,0,0,0,0,1,0", actualLines.get(3));
    assertEquals("2022-10-01,1,i-1-3,query2,0,0,0,0,0,1,1000000", actualLines.get(4));
  }

  @Test
  void testWeeklyCsv() throws Exception {
    ContentQueryMetricsJob job = createJob();
    job.weekly = true;
    job.outputCsv = true;
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    Tables.createCatalogAndDatabase(tableEnv);

    waitForDone(job.createOperators(tableEnv, joinedImpressions, attributedActions));

    // Asserts that some files.  We cannot assert the file contents though because Flink doesn't
    // output all of the rows to
    // the File sink before finishing.
    File[] filesArray =
        new File(tempDir, "blue/etl/weekly_content_query_joined_metrics_csv/dt=2022-09-25/")
            .listFiles();
    List<File> files =
        Arrays.stream(filesArray)
            .filter(Predicate.not(ContentQueryMetricsJobMiniclusterTest::isSuccessFile))
            .collect(Collectors.toList());
    assertEquals(1, files.size());
    String filePath = files.get(0).getPath();
    assertTrue(
        filePath.endsWith(".csv"), () -> "File should end with a `.csv` suffix, " + filePath);

    List<String> actualLines = Files.readAllLines(Paths.get(files.get(0).getPath()));
    assertEquals(5, actualLines.size());
    assertEquals(
        "date,platform_id,content_id,search_query,impression_count,impression_position_sum,navigate_count,add_to_cart_count,checkout_count,purchase_count,gmv_usd_micros",
        actualLines.get(0));
    assertEquals("2022-09-25,1,i-1-1,query1,1,5,1,0,0,0,0", actualLines.get(1));
    assertEquals("2022-09-25,1,i-1-2,query2,3,10,0,1,2,2,2000000", actualLines.get(2));
    assertEquals("2022-09-25,1,i-1-3,query3,0,0,0,0,0,1,0", actualLines.get(3));
    assertEquals("2022-09-25,1,i-1-3,query3,0,0,0,0,0,1,0", actualLines.get(3));
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
