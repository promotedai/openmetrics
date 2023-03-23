package ai.promoted.metrics.logprocessor.job.join;

import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.assertAvroFiles;
import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.getNestedField;
import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.loadAvroRecords;
import static ai.promoted.metrics.logprocessor.common.testing.AvroProtoUtils.toFixedAvroGenericRecord;
import static ai.promoted.metrics.logprocessor.common.testing.FileAsserts.assertPartFilesExist;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestIterator;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestIteratorOptions;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.Content;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDB;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDBFactory;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentType;
import ai.promoted.metrics.logprocessor.common.functions.UserInfoUtil;
import ai.promoted.metrics.logprocessor.common.functions.content.common.ContentAPILoader;
import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobMiniclusterTest;
import ai.promoted.metrics.logprocessor.common.testing.AvroAsserts;
import ai.promoted.metrics.logprocessor.common.testing.MiniClusterExtension;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.CurrencyCode;
import ai.promoted.proto.common.Money;
import ai.promoted.proto.common.Properties;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.delivery.DeliveryExecution;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.ExecutionServer;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.Cart;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.HiddenApiRequest;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.Preconditions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Warning: this test runs the stream jobs as batch. Flink stream minicluster tests don't
 * automatically checkpoint at the end. TODO - add link to thread about this.
 */
@ExtendWith(MiniClusterExtension.class)
public class FlatOutputJobMiniclusterTest extends BaseJobMiniclusterTest<FlatOutputJob> {

  // TODO - add other tests for very little data.  The test code requires lists to be not empty.

  private FlatOutputJob job;
  private long timeMillis;
  private String dt;
  private String hour;
  private LogRequestFactory.DetailLevel detailLevel;
  private Set<String> expectedEtlDirs;
  private Set<String> expectedDroppedDirs;
  private Set<String> expectedDebugDirs;
  private Set<String> expectedLateDirs;

  // References to the fake data gen outputs.
  private List<User> user;
  private List<View> view;
  private List<DeliveryLog> deliveryLog;
  private List<Impression> impression;
  private List<Action> action;
  private ContentDB contentDb;

  private static List<String> filterOutByPrefix(File basePath, String prefix) {
    return Arrays.stream(
            Objects.requireNonNullElse(
                basePath.listFiles((dir, name) -> name.startsWith(prefix)), new File[0]))
        .map(File::getName)
        .collect(Collectors.toList());
  }

  @Override
  @BeforeEach
  public void setUp() {
    super.setUp();
    job = createJob();
    job.maxParallelism = 1;
    // TODO(PRO-376): enable this to restore latest impressions coverage.
    // TODO(PRO-813): enable synthetic user support in tests.
    job.addLatestImpressions = false;

    timeMillis = 1601596151000L;
    dt = "2020-10-01";
    hour = "23";
    detailLevel = LogRequestFactory.DetailLevel.PARTIAL;

    expectedEtlDirs =
        Sets.newHashSet("flat_response_insertion", "joined_impression", "joined_action");
    expectedDroppedDirs = Sets.newHashSet();
    expectedDebugDirs =
        Sets.newHashSet(
            "debug_tiny_event", "debug_rhs_tiny_action", "debug_partial_response_insertion");
    expectedLateDirs = Sets.newHashSet();
    user = null;
    view = null;
    deliveryLog = null;
    impression = null;
    action = null;
    contentDb = null;
  }

  @Override
  protected FlatOutputJob createJob() {
    FlatOutputJob job = new FlatOutputJob();
    job.s3.rootPath = tempDir.getAbsolutePath();
    // Checkpoint more frequently so we don't hit part files.
    job.checkpointInterval = Duration.ofSeconds(1);
    job.checkpointTimeout = Duration.ofSeconds(60);
    job.writeJoinedEventsToKafka = false;
    job.disableAutoGeneratedUIDs = false;
    job.configureExecutionEnvironment(env, 1, 0);
    return job;
  }

  // Also tests a different job label.  We merged this test case together to have fewer tests (since
  // each test takes a few second).
  @DisplayName("One full event > Most fields set test")
  @ParameterizedTest(name = "Date test {0}, {1}")
  @CsvSource({
    "PARTIAL, before utc date, 1601596151000, 2020-10-01, 23",
    "FULL, after utc date, 1601603351000, 2020-10-02, 01",
  })
  void oneFullEvent(
      LogRequestFactory.DetailLevel detailLevel,
      String testName,
      long timeMillis,
      String dt,
      String hour)
      throws Exception {
    this.detailLevel = detailLevel;
    this.timeMillis = timeMillis;
    this.dt = dt;
    this.hour = hour;

    job.jobLabel = "blue";
    execute(
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1)
            .setMiniSdkRate(0.0f)
            .setShadowTrafficRate(0.0f));

    assertEquals(1, user.size());
    assertEquals(1, view.size());
    assertEquals(1, deliveryLog.size());
    assertEquals(1, impression.size());
    assertEquals(1, action.size());

    // Change the tempDir to point to the blue child directory.
    tempDir = new File(tempDir, "blue");

    assertSideOutputCount();
    // CombinedDeliveryLog delays the Request by the window.
    long logTimestamp = deliveryLog.get(0).getRequest().getTiming().getEventApiTimestamp();
    JoinedEvent.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    assertJoinedOneFullImpressionAndAction(joinedImpressionBuilder, logTimestamp);
  }

  @Test
  void testHudiOutput() throws Exception {
    String defaultDatabase = "flat_db";
    job.contentApiSegment.contentIdFieldKeys = ImmutableList.of(ContentType.STORE.id);
    job.writeHudiTables = true;
    job.hudiOutput.hudiBasePath = tempDir.getAbsolutePath() + "/hudi/";
    job.hudiOutput.hudiCatalogName = "flat_tables";
    job.hudiOutput.hudiDefaultDatabaseName = defaultDatabase;
    job.hudiOutput.enableHudiGlueSync = false;
    job.jobLabel = "blue";
    job.hudiSideDatabaseName = "etl_side";
    setupJoinInputs(LogRequestFactory.createStoreToItemBuilder());

    executeJoinEvents();

    waitForDone(env.execute("join-event"));
    String catalogPath = new File(tempDir, "hudi/catalog/").getAbsolutePath();
    String createCatalogSql =
        String.format(
            "create catalog hudi with ('catalog.path'='%s', 'type'='hudi', 'default-database'='%s')",
            catalogPath, defaultDatabase + "_blue");
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    tableEnv.executeSql(createCatalogSql);
    tableEnv.executeSql("use catalog hudi");
    List<String> expectedDatabaseList = List.of(defaultDatabase + "_blue", "etl_side_blue");
    List<String> databaseList =
        ImmutableList.copyOf(tableEnv.executeSql("show databases").collect()).stream()
            .map(row -> row.getField(0).toString())
            .collect(Collectors.toList());
    assertThat(databaseList).containsExactlyElementsIn(expectedDatabaseList);
    List<String> expectedFlatTables =
        List.of("flat_response_insertion", "joined_action", "joined_impression");
    List<String> flatTables =
        ImmutableList.copyOf(tableEnv.executeSql("show tables").collect()).stream()
            .map(row -> row.getField(0).toString())
            .collect(Collectors.toList());
    assertThat(flatTables).containsExactlyElementsIn(expectedFlatTables);
    tableEnv.executeSql("use etl_side_blue");
    List<String> sideTables =
        ImmutableList.copyOf(tableEnv.executeSql("show tables").collect()).stream()
            .map(row -> row.getField(0).toString())
            .collect(Collectors.toList());
    List<String> expectedSideTables =
        List.of(
            "debug_partial_response_insertion",
            "debug_rhs_tiny_action",
            "debug_tiny_event",
            "dropped_delivery_log_is_bot",
            "dropped_delivery_log_should_not_join",
            "dropped_ignore_usage_flat_response_insertion",
            "dropped_ignore_usage_joined_action",
            "dropped_ignore_usage_joined_impression",
            "dropped_impression_actions",
            "dropped_insertion_impressions",
            "dropped_merge_action_details",
            "dropped_merge_impression_details",
            "dropped_redundant_impression",
            "dropped_view_is_bot",
            "dropped_view_responseinsertions",
            "late_impression_actions",
            "late_insertion_impressions",
            "late_view_responseinsertions",
            "mismatch_error",
            "validation_error");
    assertThat(sideTables).containsExactlyElementsIn(expectedSideTables);
  }

  @DisplayName("Store page to item page (no impression) to checkout to purchase")
  @Test
  void storeToItem() throws Exception {
    job.skipViewJoin = true;
    job.contentApiSegment.contentIdFieldKeys = ImmutableList.of(ContentType.STORE.id);

    setupJoinInputs(LogRequestFactory.createStoreToItemBuilder());

    // TODO - change fake data generator to support this case.
    assertEquals(1, impression.size());
    assertEquals(4, action.size());

    executeJoinEvents();

    waitForDone(env.execute("join-event"));

    assertEtlOutputCount();
    assertSideOutputCount();

    // CombinedDeliveryLog delays the Request by the window.
    JoinedEvent.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    // I don't know why this is happening.  Even having skipViewJoin=false, the join is messed up.
    // Maybe there is an issue with view attribution.
    joinedImpressionBuilder.clearView();
    joinedImpressionBuilder.getIdsBuilder().clearSessionId();
    joinedImpressionBuilder.getIdsBuilder().clearViewId();

    assertJoinedImpression(joinedImpressionBuilder.clone());

    // Assert that we have 3 Actions.
    String actionPath = String.format("etl/joined_action/dt=%s/hour=%s", dt, hour);
    File[] actionFiles = assertPartFilesExist(new File(tempDir, actionPath));
    Set<GenericRecord> actualActions = loadAvroRecords(actionFiles);
    assertEquals(4, actualActions.size());
    Map<String, GenericRecord> actionIdToAction =
        actualActions.stream()
            .collect(
                Collectors.toMap(a -> (String) getNestedField(a, "action", "action_id"), a -> a));

    assertEquals(
        ImmutableSet.of(
            "66666666-6666-6666-0000-000000000001",
            "66666666-6666-6666-0000-000000000003",
            "66666666-6666-6666-0000-000000000004",
            "66666666-6666-6666-0000-000000000005"),
        actionIdToAction.keySet());

    JoinedEvent.Builder expectedNavigate =
        joinedImpressionBuilder.clone().setAction(toExpectedActionField(action.get(0)));
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedNavigate.clone().build()),
        actionIdToAction.get("66666666-6666-6666-0000-000000000001"),
        "navigate");
    JoinedEvent.Builder expectedAddToCart = expectedNavigate.clone();
    expectedAddToCart
        .getActionBuilder()
        .setContentId("i-1-1")
        .setActionType(ActionType.ADD_TO_CART)
        .setActionId("66666666-6666-6666-0000-000000000003");
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedAddToCart.build()),
        actionIdToAction.get("66666666-6666-6666-0000-000000000003"),
        "addToCart");
    JoinedEvent.Builder expectedCheckout = expectedNavigate.clone();
    expectedCheckout
        .getActionBuilder()
        .setContentId("i-1-1")
        .setActionType(ActionType.CHECKOUT)
        .setActionId("66666666-6666-6666-0000-000000000004")
        .setSingleCartContent(
            CartContent.newBuilder()
                .setContentId("i-1-1")
                .setQuantity(1)
                .setPricePerUnit(
                    Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(5000000L)))
        .setCart(
            Cart.newBuilder()
                .addContents(
                    CartContent.newBuilder()
                        .setContentId("i-1-1")
                        .setQuantity(1)
                        .setPricePerUnit(
                            Money.newBuilder()
                                .setCurrencyCode(CurrencyCode.USD)
                                .setAmountMicros(5000000L))));
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedCheckout.build()),
        actionIdToAction.get("66666666-6666-6666-0000-000000000004"),
        "checkout");
    JoinedEvent.Builder expectedPurchase = expectedNavigate.clone();
    expectedPurchase
        .getActionBuilder()
        .setContentId("i-1-1")
        .setActionType(ActionType.PURCHASE)
        .setActionId("66666666-6666-6666-0000-000000000005")
        .setSingleCartContent(
            CartContent.newBuilder()
                .setContentId("i-1-1")
                .setQuantity(1)
                .setPricePerUnit(
                    Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(5000000L)))
        .setCart(
            Cart.newBuilder()
                .addContents(
                    CartContent.newBuilder()
                        .setContentId("i-1-1")
                        .setQuantity(1)
                        .setPricePerUnit(
                            Money.newBuilder()
                                .setCurrencyCode(CurrencyCode.USD)
                                .setAmountMicros(5000000L))));
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedPurchase.build()),
        actionIdToAction.get("66666666-6666-6666-0000-000000000005"),
        "purchase");
  }

  @DisplayName("Shopping cart")
  @Test
  void storeToItem_cart() throws Exception {
    job.skipViewJoin = true;
    job.contentApiSegment.contentIdFieldKeys = ImmutableList.of(ContentType.STORE.id);

    LogRequestIteratorOptions options = LogRequestFactory.createItemShoppingCartBuilder().build();
    contentDb =
        ContentDBFactory.create(LogRequestIterator.PLATFORM_ID, options.contentDBFactoryOptions());
    List<LogRequest> logRequests = ImmutableList.copyOf(new LogRequestIterator(options, contentDb));
    setupJoinInputs(logRequests);

    Map<ActionType, Long> inputActionTypeToCount =
        action.stream()
            .collect(Collectors.groupingBy(Action::getActionType, Collectors.counting()));
    assertEquals(
        ImmutableMap.of(
            ActionType.NAVIGATE,
            16L,
            ActionType.CHECKOUT,
            4L,
            ActionType.PURCHASE,
            4L,
            ActionType.ADD_TO_CART,
            4L),
        inputActionTypeToCount);

    executeJoinEvents();
    waitForDone(env.execute("join-event"));

    assertEtlOutputCount();
    assertSideOutputCount();

    String impressionPath = String.format("etl/joined_impression/dt=%s/hour=%s", dt, hour);
    File[] impressionFiles = assertPartFilesExist(new File(tempDir, impressionPath));
    Set<GenericRecord> actualImpressions = loadAvroRecords(impressionFiles);
    assertEquals(16, actualImpressions.size());

    // Assert that we have 3 Actions.
    String actionPath = String.format("etl/joined_action/dt=%s/hour=%s", dt, hour);
    File[] actionFiles = assertPartFilesExist(new File(tempDir, actionPath));
    Set<GenericRecord> actualActions = loadAvroRecords(actionFiles);
    assertEquals(52, actualActions.size());

    Map<String, Long> actionTypeToCount =
        actualActions.stream()
            .collect(
                Collectors.groupingBy(
                    action -> getNestedField(action, "action", "action_type").toString(),
                    Collectors.counting()));
    assertEquals(
        ImmutableMap.of("ADD_TO_CART", 4L, "NAVIGATE", 16L, "CHECKOUT", 16L, "PURCHASE", 16L),
        actionTypeToCount);

    Set<String> checkoutIds =
        actualActions.stream()
            .filter(a -> getNestedField(a, "action", "action_type").toString().equals("CHECKOUT"))
            .map(a -> (String) getNestedField(a, "action", "action_id"))
            .collect(Collectors.toSet());

    assertEquals(
        ImmutableSet.of(
            "66666666-6666-6666-0000-00000000001a",
            "66666666-6666-6666-0000-00000000001b",
            "66666666-6666-6666-0000-00000000001c",
            "66666666-6666-6666-0000-000000000019"),
        checkoutIds);

    // CombinedDeliveryLog delays the Request by the window.
    JoinedEvent.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    joinedImpressionBuilder
        .getIdsBuilder()
        .setLogUserId("00000000-0000-0000-0000-000000000001")
        .clearSessionId()
        .setRequestId("33333333-3333-3333-0000-000000000002")
        .setImpressionId("55555555-5555-5555-0000-000000000005")
        .setInsertionId("44444444-4444-4444-0000-000000000005")
        .setViewId("22222222-2222-2222-0000-000000000002");
    // I don't know why this is happening.  Even having skipViewJoin=false, the join is messed up.
    // Maybe there is an issue with view attribution.
    joinedImpressionBuilder.clearView();
    joinedImpressionBuilder.setRequestInsertion(
        createExpectedItemRequestInsertion("s-1-1", "i-1-1"));
    joinedImpressionBuilder
        .getRequestBuilder()
        .setClientRequestId("client-33333333-3333-3333-0000-000000000002");
    joinedImpressionBuilder.getResponseInsertionBuilder().setContentId("i-1-1");
    joinedImpressionBuilder.getImpressionBuilder().setContentId("i-1-1");

    JoinedEvent.Builder expectedNavigate =
        joinedImpressionBuilder.clone().setAction(toExpectedActionField(action.get(0)));

    JoinedEvent.Builder expectedCheckout1 = expectedNavigate.clone();
    expectedCheckout1
        .getActionBuilder()
        // Action.content_id is the original content_id on the raw logged Action.
        .setContentId("i-1-221")
        .setActionType(ActionType.CHECKOUT)
        .setActionId("66666666-6666-6666-0000-00000000001c")
        .setSingleCartContent(
            CartContent.newBuilder()
                .setContentId("i-1-1")
                .setQuantity(1)
                .setPricePerUnit(
                    Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(5000000L)))
        .setCart(
            Cart.newBuilder()
                .addContents(
                    CartContent.newBuilder()
                        .setContentId("i-1-1")
                        .setQuantity(1)
                        .setPricePerUnit(
                            Money.newBuilder()
                                .setCurrencyCode(CurrencyCode.USD)
                                .setAmountMicros(5000000L)))
                .addContents(
                    CartContent.newBuilder()
                        .setContentId("i-1-181")
                        .setQuantity(4)
                        .setPricePerUnit(
                            Money.newBuilder()
                                .setCurrencyCode(CurrencyCode.USD)
                                .setAmountMicros(4000000L)))
                .addContents(
                    CartContent.newBuilder()
                        .setContentId("i-1-201")
                        .setQuantity(3)
                        .setPricePerUnit(
                            Money.newBuilder()
                                .setCurrencyCode(CurrencyCode.USD)
                                .setAmountMicros(2000000L)))
                .addContents(
                    CartContent.newBuilder()
                        .setContentId("i-1-221")
                        .setQuantity(1)
                        .setPricePerUnit(
                            Money.newBuilder()
                                .setCurrencyCode(CurrencyCode.USD)
                                .setAmountMicros(4000000L))));

    Map<String, GenericRecord> contentIdToCheckouts =
        actualActions.stream()
            .filter(
                a ->
                    getNestedField(a, "action", "action_id")
                        .equals("66666666-6666-6666-0000-00000000001c"))
            .collect(
                Collectors.toMap(
                    a -> (String) getNestedField(a, "response_insertion", "content_id"), a -> a));
    assertEquals(4, contentIdToCheckouts.size());
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedCheckout1.clone().build()),
        contentIdToCheckouts.get("i-1-1"),
        "checkout-1");

    JoinedEvent.Builder expectedCheckout181 = expectedCheckout1.clone();
    expectedCheckout181
        .getIdsBuilder()
        .setRequestId("33333333-3333-3333-0000-000000000003")
        .setImpressionId("55555555-5555-5555-0000-000000000009")
        .setInsertionId("44444444-4444-4444-0000-000000000009")
        .setViewId("22222222-2222-2222-0000-000000000003");
    expectedCheckout181
        .getRequestBuilder()
        .setClientRequestId("client-33333333-3333-3333-0000-000000000003");
    expectedCheckout181.setRequestInsertion(
        createExpectedItemRequestInsertion("s-1-10", "i-1-181"));
    expectedCheckout181
        .getApiExecutionInsertionBuilder()
        .getFeatureStageBuilder()
        .getFeaturesBuilder()
        .putNumeric(100, 0.5f);
    expectedCheckout181.getResponseInsertionBuilder().setContentId("i-1-181");
    expectedCheckout181.getImpressionBuilder().setContentId("i-1-181");
    expectedCheckout181
        .getActionBuilder()
        .getSingleCartContentBuilder()
        .setContentId("i-1-181")
        .setQuantity(4)
        .setPricePerUnit(
            Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(4000000L));

    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedCheckout181.clone().build()),
        contentIdToCheckouts.get("i-1-181"),
        "checkout-181");

    JoinedEvent.Builder expectedCheckout201 = expectedCheckout1.clone();
    expectedCheckout201
        .getIdsBuilder()
        .setRequestId("33333333-3333-3333-0000-000000000004")
        .setImpressionId("55555555-5555-5555-0000-00000000000d")
        .setInsertionId("44444444-4444-4444-0000-00000000000d")
        .setViewId("22222222-2222-2222-0000-000000000004");
    expectedCheckout201
        .getRequestBuilder()
        .setClientRequestId("client-33333333-3333-3333-0000-000000000004");
    expectedCheckout201.setRequestInsertion(
        createExpectedItemRequestInsertion("s-1-11", "i-1-201"));
    expectedCheckout201
        .getApiExecutionInsertionBuilder()
        .getFeatureStageBuilder()
        .getFeaturesBuilder()
        .putNumeric(100, 0.3f);
    expectedCheckout201.getResponseInsertionBuilder().setContentId("i-1-201");
    expectedCheckout201.getImpressionBuilder().setContentId("i-1-201");
    expectedCheckout201
        .getActionBuilder()
        .getSingleCartContentBuilder()
        .setContentId("i-1-201")
        .setQuantity(3)
        .setPricePerUnit(
            Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(2000000L));

    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedCheckout201.clone().build()),
        contentIdToCheckouts.get("i-1-201"),
        "checkout-201");

    JoinedEvent.Builder expectedCheckout221 = expectedCheckout1.clone();
    expectedCheckout221
        .getIdsBuilder()
        .setRequestId("33333333-3333-3333-0000-000000000005")
        .setImpressionId("55555555-5555-5555-0000-000000000011")
        .setInsertionId("44444444-4444-4444-0000-000000000011")
        .setViewId("22222222-2222-2222-0000-000000000005");
    expectedCheckout221
        .getRequestBuilder()
        .setClientRequestId("client-33333333-3333-3333-0000-000000000005");
    expectedCheckout221.setRequestInsertion(
        createExpectedItemRequestInsertion("s-1-12", "i-1-221"));
    expectedCheckout221.getResponseInsertionBuilder().setContentId("i-1-221");
    expectedCheckout221.getImpressionBuilder().setContentId("i-1-221");
    expectedCheckout221
        .getActionBuilder()
        .getSingleCartContentBuilder()
        .setContentId("i-1-221")
        .setQuantity(1)
        .setPricePerUnit(
            Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(4000000L));

    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedCheckout221.clone().build()),
        contentIdToCheckouts.get("i-1-221"),
        "checkout-221");

    // Assert purchases.
    Set<String> purchaseIds =
        actualActions.stream()
            .filter(a -> getNestedField(a, "action", "action_type").toString().equals("PURCHASE"))
            .map(a -> (String) getNestedField(a, "action", "action_id"))
            .collect(Collectors.toSet());

    assertEquals(
        ImmutableSet.of(
            "66666666-6666-6666-0000-00000000001d",
            "66666666-6666-6666-0000-00000000001e",
            "66666666-6666-6666-0000-00000000001f",
            "66666666-6666-6666-0000-000000000020"),
        purchaseIds);

    Map<String, GenericRecord> contentIdToPurchases =
        actualActions.stream()
            .filter(
                a ->
                    getNestedField(a, "action", "action_id")
                        .equals("66666666-6666-6666-0000-000000000020"))
            .collect(
                Collectors.toMap(
                    a -> (String) getNestedField(a, "response_insertion", "content_id"), a -> a));
    assertEquals(4, contentIdToPurchases.size());

    JoinedEvent.Builder expectedPurchase1 = expectedCheckout1.clone();
    expectedPurchase1.getActionBuilder().setActionType(ActionType.PURCHASE);
    expectedPurchase1.getActionBuilder().setActionId("66666666-6666-6666-0000-000000000020");
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedPurchase1.build()),
        contentIdToPurchases.get("i-1-1"),
        "purchase-1");

    JoinedEvent.Builder expectedPurchase181 = expectedCheckout181.clone();
    expectedPurchase181.getActionBuilder().setActionType(ActionType.PURCHASE);
    expectedPurchase181.getActionBuilder().setActionId("66666666-6666-6666-0000-000000000020");
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedPurchase181.build()),
        contentIdToPurchases.get("i-1-181"),
        "purchase-181");

    JoinedEvent.Builder expectedPurchase201 = expectedCheckout201.clone();
    expectedPurchase201.getActionBuilder().setActionType(ActionType.PURCHASE);
    expectedPurchase201.getActionBuilder().setActionId("66666666-6666-6666-0000-000000000020");
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedPurchase201.build()),
        contentIdToPurchases.get("i-1-201"),
        "purchase-201");

    JoinedEvent.Builder expectedPurchase221 = expectedCheckout221.clone();
    expectedPurchase221.getActionBuilder().setActionType(ActionType.PURCHASE);
    expectedPurchase221.getActionBuilder().setActionId("66666666-6666-6666-0000-000000000020");
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedPurchase221.build()),
        contentIdToPurchases.get("i-1-221"),
        "purchase-221");
  }

  @DisplayName("Shopping cart > store insertion")
  @Test
  void storeToItem_cart_storeInsertion() throws Exception {
    job.skipViewJoin = true;
    job.contentApiSegment.contentIdFieldKeys = ImmutableList.of(ContentType.STORE.id);

    LogRequestIteratorOptions options =
        LogRequestFactory.createStoreInsertionItemShoppingCartBuilder().build();
    contentDb =
        ContentDBFactory.create(LogRequestIterator.PLATFORM_ID, options.contentDBFactoryOptions());
    List<LogRequest> logRequests = ImmutableList.copyOf(new LogRequestIterator(options, contentDb));
    setupJoinInputs(logRequests);

    Map<ActionType, Long> inputActionTypeToCount =
        action.stream()
            .collect(Collectors.groupingBy(Action::getActionType, Collectors.counting()));
    assertEquals(
        ImmutableMap.of(
            ActionType.NAVIGATE,
            4L,
            ActionType.CHECKOUT,
            4L,
            ActionType.PURCHASE,
            4L,
            ActionType.ADD_TO_CART,
            4L),
        inputActionTypeToCount);

    executeJoinEvents();
    waitForDone(env.execute("join-event"));

    assertEtlOutputCount();
    assertSideOutputCount();

    String impressionPath = String.format("etl/joined_impression/dt=%s/hour=%s", dt, hour);
    File[] impressionFiles = assertPartFilesExist(new File(tempDir, impressionPath));
    Set<GenericRecord> actualImpressions = loadAvroRecords(impressionFiles);
    assertEquals(4, actualImpressions.size());

    // Assert that we have 3 Actions.
    String actionPath = String.format("etl/joined_action/dt=%s/hour=%s", dt, hour);
    File[] actionFiles = assertPartFilesExist(new File(tempDir, actionPath));
    Set<GenericRecord> actualActions = loadAvroRecords(actionFiles);
    assertEquals(40, actualActions.size());

    Map<String, Long> actionTypeToCount =
        actualActions.stream()
            .collect(
                Collectors.groupingBy(
                    action -> getNestedField(action, "action", "action_type").toString(),
                    Collectors.counting()));

    assertEquals(
        ImmutableMap.of("ADD_TO_CART", 4L, "NAVIGATE", 4L, "CHECKOUT", 16L, "PURCHASE", 16L),
        actionTypeToCount);

    Set<String> checkoutIds =
        actualActions.stream()
            .filter(a -> getNestedField(a, "action", "action_type").toString().equals("CHECKOUT"))
            .map(a -> (String) getNestedField(a, "action", "action_id"))
            .collect(Collectors.toSet());

    assertEquals(
        ImmutableSet.of(
            "66666666-6666-6666-0000-00000000001a",
            "66666666-6666-6666-0000-00000000001b",
            "66666666-6666-6666-0000-00000000001c",
            "66666666-6666-6666-0000-000000000019"),
        checkoutIds);

    Map<String, GenericRecord> contentIdToCheckouts =
        actualActions.stream()
            .filter(
                a ->
                    getNestedField(a, "action", "action_id")
                        .equals("66666666-6666-6666-0000-00000000001c"))
            .collect(
                Collectors.toMap(
                    a -> (String) getNestedField(a, "response_insertion", "content_id"), a -> a));
    assertEquals(4, contentIdToCheckouts.size());

    assertEquals(
        ImmutableSet.of("s-1-1", "s-1-11", "s-1-12", "s-1-10"), contentIdToCheckouts.keySet());

    // CombinedDeliveryLog delays the Request by the window.
    JoinedEvent.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    joinedImpressionBuilder
        .getIdsBuilder()
        .setLogUserId("00000000-0000-0000-0000-000000000001")
        .clearSessionId()
        .setRequestId("33333333-3333-3333-0000-000000000001")
        .setImpressionId("55555555-5555-5555-0000-000000000001")
        .setInsertionId("44444444-4444-4444-0000-000000000001")
        .setViewId("22222222-2222-2222-0000-000000000001");
    // I don't know why this is happening.  Even having skipViewJoin=false, the join is messed up.
    // Maybe there is an issue with view attribution.
    joinedImpressionBuilder.clearView();
    joinedImpressionBuilder
        .getRequestBuilder()
        .setClientRequestId("client-33333333-3333-3333-0000-000000000001");
    joinedImpressionBuilder.setRequestInsertion(createExpectedStoreRequestInsertion("s-1-1"));
    joinedImpressionBuilder.getResponseInsertionBuilder().setContentId("s-1-1");
    joinedImpressionBuilder.getImpressionBuilder().setContentId("s-1-1");

    JoinedEvent.Builder expectedNavigate =
        joinedImpressionBuilder.clone().setAction(toExpectedActionField(action.get(0)));

    JoinedEvent.Builder expectedCheckout1 = expectedNavigate.clone();
    expectedCheckout1
        .getActionBuilder()
        // Action.content_id is the original content_id on the raw logged Action.
        .setContentId("i-1-221")
        .setActionType(ActionType.CHECKOUT)
        .setActionId("66666666-6666-6666-0000-00000000001c")
        .setSingleCartContent(
            CartContent.newBuilder()
                .setContentId("i-1-1")
                .setQuantity(1)
                .setPricePerUnit(
                    Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(5000000L)))
        .setCart(
            Cart.newBuilder()
                .addContents(
                    CartContent.newBuilder()
                        .setContentId("i-1-1")
                        .setQuantity(1)
                        .setPricePerUnit(
                            Money.newBuilder()
                                .setCurrencyCode(CurrencyCode.USD)
                                .setAmountMicros(5000000L)))
                .addContents(
                    CartContent.newBuilder()
                        .setContentId("i-1-181")
                        .setQuantity(4)
                        .setPricePerUnit(
                            Money.newBuilder()
                                .setCurrencyCode(CurrencyCode.USD)
                                .setAmountMicros(4000000L)))
                .addContents(
                    CartContent.newBuilder()
                        .setContentId("i-1-201")
                        .setQuantity(3)
                        .setPricePerUnit(
                            Money.newBuilder()
                                .setCurrencyCode(CurrencyCode.USD)
                                .setAmountMicros(2000000L)))
                .addContents(
                    CartContent.newBuilder()
                        .setContentId("i-1-221")
                        .setQuantity(1)
                        .setPricePerUnit(
                            Money.newBuilder()
                                .setCurrencyCode(CurrencyCode.USD)
                                .setAmountMicros(4000000L))));

    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedCheckout1.clone().build()),
        contentIdToCheckouts.get("s-1-1"),
        "checkout-1");

    // Specific checks since we already have full tests.
    GenericRecord actualCheckout10 = contentIdToCheckouts.get("s-1-10");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualCheckout10, "ids", "request_id"));
    assertEquals(
        "44444444-4444-4444-0000-000000000002",
        getNestedField(actualCheckout10, "ids", "insertion_id"));
    assertEquals(
        "55555555-5555-5555-0000-000000000002",
        getNestedField(actualCheckout10, "ids", "impression_id"));
    assertEquals("s-1-10", getNestedField(actualCheckout10, "response_insertion", "content_id"));
    assertEquals("s-1-10", getNestedField(actualCheckout10, "impression", "content_id"));
    assertEquals(
        "i-1-181", getNestedField(actualCheckout10, "action", "single_cart_content", "content_id"));

    GenericRecord actualCheckout11 = contentIdToCheckouts.get("s-1-11");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualCheckout11, "ids", "request_id"));
    assertEquals(
        "44444444-4444-4444-0000-000000000003",
        getNestedField(actualCheckout11, "ids", "insertion_id"));
    assertEquals(
        "55555555-5555-5555-0000-000000000003",
        getNestedField(actualCheckout11, "ids", "impression_id"));
    assertEquals("s-1-11", getNestedField(actualCheckout11, "response_insertion", "content_id"));
    assertEquals("s-1-11", getNestedField(actualCheckout11, "impression", "content_id"));
    assertEquals(
        "i-1-201", getNestedField(actualCheckout11, "action", "single_cart_content", "content_id"));

    GenericRecord actualCheckout12 = contentIdToCheckouts.get("s-1-12");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualCheckout12, "ids", "request_id"));
    assertEquals(
        "44444444-4444-4444-0000-000000000004",
        getNestedField(actualCheckout12, "ids", "insertion_id"));
    assertEquals(
        "55555555-5555-5555-0000-000000000004",
        getNestedField(actualCheckout12, "ids", "impression_id"));
    assertEquals("s-1-12", getNestedField(actualCheckout12, "response_insertion", "content_id"));
    assertEquals("s-1-12", getNestedField(actualCheckout12, "impression", "content_id"));
    assertEquals(
        "i-1-221", getNestedField(actualCheckout12, "action", "single_cart_content", "content_id"));

    Set<String> purchaseIds =
        actualActions.stream()
            .filter(a -> getNestedField(a, "action", "action_type").toString().equals("PURCHASE"))
            .map(a -> (String) getNestedField(a, "action", "action_id"))
            .collect(Collectors.toSet());

    assertEquals(
        ImmutableSet.of(
            "66666666-6666-6666-0000-00000000001f",
            "66666666-6666-6666-0000-00000000001e",
            "66666666-6666-6666-0000-00000000001d",
            "66666666-6666-6666-0000-000000000020"),
        purchaseIds);

    Map<String, GenericRecord> contentIdToPurchases =
        actualActions.stream()
            .filter(
                a ->
                    getNestedField(a, "action", "action_id")
                        .equals("66666666-6666-6666-0000-00000000001d"))
            .collect(
                Collectors.toMap(
                    a -> (String) getNestedField(a, "response_insertion", "content_id"), a -> a));
    assertEquals(4, contentIdToCheckouts.size());

    assertEquals(
        ImmutableSet.of("s-1-1", "s-1-11", "s-1-12", "s-1-10"), contentIdToPurchases.keySet());

    GenericRecord actualPurchase1 = contentIdToPurchases.get("s-1-1");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualPurchase1, "ids", "request_id"));
    assertEquals(
        "44444444-4444-4444-0000-000000000001",
        getNestedField(actualPurchase1, "ids", "insertion_id"));
    assertEquals(
        "55555555-5555-5555-0000-000000000001",
        getNestedField(actualPurchase1, "ids", "impression_id"));
    assertEquals("s-1-1", getNestedField(actualPurchase1, "response_insertion", "content_id"));
    assertEquals("s-1-1", getNestedField(actualPurchase1, "impression", "content_id"));
    assertEquals("PURCHASE", getNestedField(actualPurchase1, "action", "action_type").toString());
    assertEquals(
        "i-1-1", getNestedField(actualPurchase1, "action", "single_cart_content", "content_id"));

    GenericRecord actualPurchase10 = contentIdToPurchases.get("s-1-10");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualPurchase10, "ids", "request_id"));
    assertEquals(
        "44444444-4444-4444-0000-000000000002",
        getNestedField(actualPurchase10, "ids", "insertion_id"));
    assertEquals(
        "55555555-5555-5555-0000-000000000002",
        getNestedField(actualPurchase10, "ids", "impression_id"));
    assertEquals("s-1-10", getNestedField(actualPurchase10, "response_insertion", "content_id"));
    assertEquals("s-1-10", getNestedField(actualPurchase10, "impression", "content_id"));
    assertEquals("PURCHASE", getNestedField(actualPurchase10, "action", "action_type").toString());
    assertEquals(
        "i-1-181", getNestedField(actualPurchase10, "action", "single_cart_content", "content_id"));

    GenericRecord actualPurchase11 = contentIdToPurchases.get("s-1-11");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualPurchase11, "ids", "request_id"));
    assertEquals(
        "44444444-4444-4444-0000-000000000003",
        getNestedField(actualPurchase11, "ids", "insertion_id"));
    assertEquals(
        "55555555-5555-5555-0000-000000000003",
        getNestedField(actualPurchase11, "ids", "impression_id"));
    assertEquals("s-1-11", getNestedField(actualPurchase11, "response_insertion", "content_id"));
    assertEquals("s-1-11", getNestedField(actualPurchase11, "impression", "content_id"));
    assertEquals("PURCHASE", getNestedField(actualPurchase11, "action", "action_type").toString());
    assertEquals(
        "i-1-201", getNestedField(actualPurchase11, "action", "single_cart_content", "content_id"));

    GenericRecord actualPurchase12 = contentIdToPurchases.get("s-1-12");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualPurchase12, "ids", "request_id"));
    assertEquals(
        "44444444-4444-4444-0000-000000000004",
        getNestedField(actualPurchase12, "ids", "insertion_id"));
    assertEquals(
        "55555555-5555-5555-0000-000000000004",
        getNestedField(actualPurchase12, "ids", "impression_id"));
    assertEquals("s-1-12", getNestedField(actualPurchase12, "response_insertion", "content_id"));
    assertEquals("s-1-12", getNestedField(actualPurchase12, "impression", "content_id"));
    assertEquals("PURCHASE", getNestedField(actualPurchase12, "action", "action_type").toString());
    assertEquals(
        "i-1-221", getNestedField(actualPurchase12, "action", "single_cart_content", "content_id"));
  }

  private Insertion createExpectedStoreRequestInsertion(String storeId) {
    return Insertion.newBuilder()
        .setProperties(
            Properties.newBuilder()
                .setStruct(
                    Struct.newBuilder()
                        .putFields("storeId", Value.newBuilder().setStringValue(storeId).build())
                        .putFields("type", Value.newBuilder().setStringValue("STORE").build())
                        .build()))
        .build();
  }

  private Insertion createExpectedItemRequestInsertion(String storeId, String itemId) {
    return Insertion.newBuilder()
        .setProperties(
            Properties.newBuilder()
                .setStruct(
                    Struct.newBuilder()
                        .putFields("itemId", Value.newBuilder().setStringValue(itemId).build())
                        .putFields("storeId", Value.newBuilder().setStringValue(storeId).build())
                        .putFields("type", Value.newBuilder().setStringValue("ITEM").build())
                        .build()))
        .build();
  }

  // TODO - add tests - https://toil.kitemaker.co/se6ONh-Promoted/hdRZPv-Promoted/items/813
  // TODO(PRO-1155) - add tests for right outer join.

  @DisplayName("One full event > Combine SDK and shadow traffic DeliveryLogs")
  @Test
  void combineSdkAndShadowTrafficDeliveryLogs() throws Exception {
    execute(
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1)
            .setMiniSdkRate(1.0f)
            .setShadowTrafficRate(1.0f));

    assertSideOutputCount();
    // Gets delayed for the CombineDeliveryLog.
    long logTimestamp = deliveryLog.get(0).getRequest().getTiming().getEventApiTimestamp();
    JoinedEvent.Builder joinedImpressionBuilder =
        createExpectedJoinedImpressionBuilder()
            .setHiddenApiRequest(
                HiddenApiRequest.newBuilder()
                    .setRequestId("33333333-3333-3333-0000-000000000002")
                    .setTiming(
                        Timing.newBuilder()
                            .setClientLogTimestamp(logTimestamp)
                            .setEventApiTimestamp(logTimestamp)
                            .setLogTimestamp(logTimestamp))
                    .setClientInfo(
                        ClientInfo.newBuilder()
                            .setClientType(ClientInfo.ClientType.PLATFORM_SERVER)
                            .setTrafficType(ClientInfo.TrafficType.SHADOW)))
            .setApiExecutionInsertion(
                toExpectedExecutionInsertionField(
                    deliveryLog.get(1).getExecution().getExecutionInsertion(0)))
            .setSdkExecution(
                DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.SDK).build());
    assertJoinedOneFullImpressionAndAction(joinedImpressionBuilder, logTimestamp);
  }

  @DisplayName("Filter out ignoreUsage")
  @Test
  void filterOutIgnoreUsage() throws Exception {
    execute(
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1)
            .setMiniSdkRate(0.0f)
            .setShadowTrafficRate(0.0f)
            .setDeliveryLogTransform(
                builder -> {
                  builder.getRequestBuilder().getUserInfoBuilder().setIgnoreUsage(true);
                  return builder;
                }));

    expectedEtlDirs = ImmutableSet.of();
    assertEtlOutputCount();
    expectedDroppedDirs =
        ImmutableSet.of(
            "dropped_ignore_usage_joined_action", "dropped_ignore_usage_joined_impression");
    assertSideOutputCount();
    assertEquals(null, new File(tempDir, "etl/joined_impression").list());
    assertEquals(null, new File(tempDir, "etl/joined_action").list());
  }

  @DisplayName("Filter out non-buyer traffic")
  @Test
  void filterOutNonBuyer() throws Exception {
    // User property "is_staff" - ccc.
    long isStaffHash = 4078401918190321518L;
    job.nonBuyerUserSparseHashes = ImmutableList.of(4078401918190321518L);
    execute(
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1)
            .setMiniSdkRate(0.0f)
            .setShadowTrafficRate(0.0f)
            .setDeliveryLogTransform(
                builder -> {
                  builder
                      .getExecutionBuilder()
                      .getExecutionInsertionBuilder(0)
                      .getFeatureStageBuilder()
                      .getFeaturesBuilder()
                      .putSparseId(isStaffHash, 1L);
                  return builder;
                }));

    expectedEtlDirs = ImmutableSet.of();
    assertEtlOutputCount();
    expectedDroppedDirs =
        ImmutableSet.of("dropped_non_buyer_joined_impression", "dropped_non_buyer_joined_action");
    assertSideOutputCount();
    assertEquals(null, new File(tempDir, "etl/joined_impression").list());
    assertEquals(null, new File(tempDir, "etl/joined_action").list());
  }

  @DisplayName("Filter out bot traffic")
  @Test
  void filterOutBot() throws Exception {
    // User property "is_staff" - ccc.
    final long isStaffHash = 4078401918190321518L;
    job.nonBuyerUserSparseHashes = ImmutableList.of(isStaffHash);
    execute(
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1)
            .setMiniSdkRate(0.0f)
            .setShadowTrafficRate(0.0f)
            .setDeliveryLogTransform(
                builder -> {
                  builder
                      .getRequestBuilder()
                      .getDeviceBuilder()
                      .getBrowserBuilder()
                      .setUserAgent("google bot");
                  return builder;
                }));

    expectedEtlDirs = ImmutableSet.of();
    assertEtlOutputCount();
    // BUG - Drops ins-imp and imp-act because we filter out DeliveryLogs too early to save
    // resources.
    // We should have a more efficient way to filter these through the system.
    expectedDroppedDirs =
        ImmutableSet.of(
            "dropped_delivery_log_is_bot",
            "dropped_impression_actions",
            "dropped_insertion_impressions");
    expectedDebugDirs = ImmutableSet.of("debug_rhs_tiny_action");
    assertSideOutputCount();
    assertEquals(null, new File(tempDir, "etl/joined_impression").list());
    assertEquals(null, new File(tempDir, "etl/joined_action").list());
  }

  @DisplayName("One full event > Skip view join")
  @Test
  void skipViewJoin() throws Exception {
    job.skipViewJoin = true;
    execute(
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1)
            .setMiniSdkRate(0.0f)
            .setShadowTrafficRate(0.0f)
            .setMissingViewRate(1.0f));

    assertSideOutputCount();
    // CombinedDeliveryLog delays the Request by the window.
    long logTimestamp = deliveryLog.get(0).getRequest().getTiming().getEventApiTimestamp();
    JoinedEvent.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    // sessionId currently comes from the View (which is not set).
    joinedImpressionBuilder.getIdsBuilder().clearSessionId();
    joinedImpressionBuilder.clearView();
    assertJoinedOneFullImpressionAndAction(joinedImpressionBuilder, logTimestamp);
  }

  private void assertJoinedImpression(JoinedEvent.Builder builder) throws IOException {
    assertJoinedImpression(builder.build());
  }

  private void assertJoinedImpression(JoinedEvent joinedImpression) throws IOException {
    assertRecordInFile(
        "etl/joined_impression/dt=%s/hour=%s", joinedImpression, "etl/joined_impression");
  }

  private void assertJoinedAction(JoinedEvent.Builder builder) throws IOException {
    assertJoinedAction(builder.build());
  }

  private void assertJoinedAction(JoinedEvent joinedAction) throws IOException {
    assertRecordInFile("etl/joined_action/dt=%s/hour=%s", joinedAction, "etl/joined_action");
  }

  private void assertFlatResponseInsertion(FlatResponseInsertion.Builder builder)
      throws IOException {
    assertFlatResponseInsertion(builder.build());
  }

  private void assertFlatResponseInsertion(FlatResponseInsertion flatResponseInsertion)
      throws IOException {
    assertRecordInFile(
        "etl/flat_response_insertion/dt=%s/hour=%s",
        flatResponseInsertion, "etl/flat_response_insertion");
  }

  private void assertRecordInFile(String template, GeneratedMessageV3 record, String message)
      throws IOException {
    String path = String.format(template, dt, hour);
    File[] files = assertPartFilesExist(new File(tempDir, path));
    assertAvroFiles(toFixedAvroGenericRecord(record), files, message);
  }

  private void assertEtlOutputCount() {
    assertEquals(
        expectedEtlDirs, ImmutableSet.copyOf(filterOutByPrefix(new File(tempDir, "etl"), "")));
  }

  private void assertSideOutputCount() {
    assertEquals(0, filterOutByPrefix(new File(tempDir, "etl_side"), "invalid_").size());
    assertEquals(
        expectedLateDirs,
        ImmutableSet.copyOf(filterOutByPrefix(new File(tempDir, "etl_side"), "late_")));
    assertEquals(
        expectedDroppedDirs,
        ImmutableSet.copyOf(filterOutByPrefix(new File(tempDir, "etl_side"), "dropped_")));
    assertEquals(
        expectedDebugDirs,
        ImmutableSet.copyOf(filterOutByPrefix(new File(tempDir, "etl_side"), "debug_")));
  }

  private JoinedEvent.Builder createExpectedJoinedImpressionBuilder() {
    long logTimestamp = deliveryLog.get(0).getRequest().getTiming().getEventApiTimestamp();
    JoinedEvent.Builder joinedImpressionBuilder =
        JoinedEvent.newBuilder()
            .setIds(
                JoinedIdentifiers.newBuilder()
                    .setPlatformId(1L)
                    .setLogUserId("00000000-0000-0000-0000-000000000001")
                    .setSessionId("11111111-1111-1111-0000-000000000001")
                    .setViewId("22222222-2222-2222-0000-000000000001")
                    .setRequestId("33333333-3333-3333-0000-000000000001")
                    .setInsertionId("44444444-4444-4444-0000-000000000001")
                    .setImpressionId("55555555-5555-5555-0000-000000000001"))
            .setRequest(toExpectedRequestField(deliveryLog.get(0).getRequest(), logTimestamp))
            .setRequestInsertion(
                toExpectedRequestInsertionField(deliveryLog.get(0).getRequest().getInsertion(0)))
            .setResponse(toExpectedResponseField(Response.getDefaultInstance()))
            .setResponseInsertion(
                toExpectedResponseInsertionField(deliveryLog.get(0).getResponse().getInsertion(0)))
            .setApiExecution(
                DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.API).build())
            .setImpression(toExpectedImpressionField(impression.get(0)));
    if (!view.isEmpty()) {
      joinedImpressionBuilder.setView(toExpectedViewField(view.get(0), logTimestamp));
    }
    if (deliveryLog.get(0).getExecution().getExecutionInsertionCount() > 0) {
      joinedImpressionBuilder.setApiExecutionInsertion(
          toExpectedExecutionInsertionField(
              deliveryLog.get(0).getExecution().getExecutionInsertion(0)));
    }
    // TODO - remove the Timing from the impression block.  It's redundant with JoinedEvent.
    joinedImpressionBuilder.setTiming(
        // TODO - wtf?  Not sure how this is possible.
        impression.get(0).getTiming().toBuilder().setLogTimestamp(logTimestamp));
    UserInfoUtil.clearUserId(joinedImpressionBuilder);
    return joinedImpressionBuilder;
  }

  private void assertJoinedOneFullImpressionAndAction(
      JoinedEvent.Builder expectedJoinedImpressionBuilder, long logTimestamp) throws IOException {
    assertEtlOutputCount();
    assertJoinedImpression(expectedJoinedImpressionBuilder.clone());
    JoinedEvent.Builder joinedActionBuilder =
        expectedJoinedImpressionBuilder.clone().setAction(toExpectedActionField(action.get(0)));
    assertJoinedAction(joinedActionBuilder.clone());

    FlatResponseInsertion.Builder flatResponseInsertion =
        FlatUtil.createFlatResponseInsertion(
            ImmutableList.of(expectedJoinedImpressionBuilder.clone().build()),
            ImmutableList.of(joinedActionBuilder.clone().build()));
    try {
      assertFlatResponseInsertion(flatResponseInsertion);
    } catch (Throwable e) {
      // Track down a flake.
      throw new RuntimeException(
          "Adding extra info.  flatResponseInsertion=" + flatResponseInsertion.clone().build(), e);
    }
  }

  private JoinedEvent.Builder setExpectedUser(JoinedEvent.Builder builder, long logTimestamp) {
    User expectedFlatUser = toExpectedUserField(user.get(0), logTimestamp);
    return builder
        .setIds(builder.getIdsBuilder().clone().setUserId("userId1"))
        .setUser(expectedFlatUser);
  }

  private FlatResponseInsertion.Builder setExpectedUser(
      FlatResponseInsertion.Builder builder, long logTimestamp) {
    User expectedFlatUser = toExpectedUserField(user.get(0), logTimestamp);
    return builder
        .setIds(builder.getIdsBuilder().clone().setUserId("userId1"))
        .setUser(expectedFlatUser);
  }

  // Separate function to reduce test lines of code.  The JoinedIdentifiers code clears a bunch of
  // these fields.
  private User toExpectedUserField(User user, long logTimestamp) {
    User.Builder builder = user.toBuilder().clearPlatformId().clearUserInfo();
    builder.getTimingBuilder().setLogTimestamp(logTimestamp);
    return builder.build();
  }

  // Separate function to reduce test lines of code.  The JoinedIdentifiers code clears a bunch of
  // these fields.
  private View toExpectedViewField(View view, long logTimestamp) {
    View.Builder builder =
        view.toBuilder().clearPlatformId().clearUserInfo().clearViewId().clearSessionId();
    builder.getTimingBuilder().setLogTimestamp(logTimestamp);
    return builder.build();
  }

  // Separate function to reduce test lines of code.  The JoinedIdentifiers code clears a bunch of
  // these fields.
  private Request toExpectedRequestField(Request request, long logTimestamp) {
    Request.Builder builder =
        request.toBuilder()
            .clearPlatformId()
            .clearUserInfo()
            .clearRequestId()
            .clearViewId()
            .clearSessionId()
            .clearInsertion();
    builder.getTimingBuilder().setLogTimestamp(logTimestamp);
    return builder.build();
  }

  // Separate function to reduce test lines of code.  The JoinedIdentifiers code clears a bunch of
  // these fields.
  private Response toExpectedResponseField(Response response) {
    Response.Builder builder = response.toBuilder().clearInsertion();
    return builder.build();
  }

  // Separate function to reduce test lines of code.  The JoinedIdentifiers code clears a bunch of
  // these fields.
  private Insertion toExpectedResponseInsertionField(Insertion insertion) {
    return toBaseExpectedInsertionField(insertion);
  }

  // Separate function to reduce test lines of code.  The JoinedIdentifiers code clears a bunch of
  // these fields.
  private Insertion toExpectedRequestInsertionField(Insertion insertion) {
    return toBaseExpectedInsertionField(insertion).toBuilder().clearContentId().build();
  }

  // Separate function to reduce test lines of code.  The JoinedIdentifiers code clears a bunch of
  // these fields.
  private Insertion toExpectedExecutionInsertionField(Insertion insertion) {
    return toBaseExpectedInsertionField(insertion).toBuilder().clearContentId().build();
  }

  // Separate function to reduce test lines of code.  The JoinedIdentifiers code clears a bunch of
  // these fields.
  private Insertion toBaseExpectedInsertionField(Insertion insertion) {
    Insertion.Builder builder =
        insertion.toBuilder()
            .clearPlatformId()
            .clearUserInfo()
            .clearTiming()
            .clearSessionId()
            .clearViewId()
            .clearRequestId()
            .clearInsertionId();
    return builder.build();
  }

  // Separate function to reduce test lines of code.  The JoinedIdentifiers code clears a bunch of
  // these fields.
  private Impression toExpectedImpressionField(Impression impression) {
    Impression.Builder builder =
        impression.toBuilder()
            .clearPlatformId()
            .clearUserInfo()
            .clearImpressionId()
            .clearInsertionId()
            .clearRequestId()
            .clearViewId()
            .clearSessionId();
    builder.getTimingBuilder().setLogTimestamp(builder.getTiming().getEventApiTimestamp());
    return builder.build();
  }

  // Separate function to reduce test lines of code.  The JoinedIdentifiers code clears a bunch of
  // these fields.
  private Action toExpectedActionField(Action action) {
    Action.Builder builder =
        action.toBuilder()
            .clearPlatformId()
            .clearUserInfo()
            // We keep the action id since it's not on JoinedIdentifiers.
            .clearImpressionId()
            .clearInsertionId()
            .clearRequestId()
            .clearViewId()
            .clearSessionId();
    builder.getTimingBuilder().setLogTimestamp(builder.getTiming().getEventApiTimestamp());
    return builder.build();
  }

  private void execute(LogRequestIteratorOptions.Builder options) throws Exception {
    setupJoinInputs(options);
    executeJoinEvents();
    waitForDone(env.execute("join-event"));
  }

  private void setupJoinInputs(LogRequestIteratorOptions.Builder optionsBuilder) {
    LogRequestIteratorOptions options = optionsBuilder.build();
    contentDb =
        ContentDBFactory.create(LogRequestIterator.PLATFORM_ID, options.contentDBFactoryOptions());
    List<LogRequest> logRequests = ImmutableList.copyOf(new LogRequestIterator(options, contentDb));
    setupJoinInputs(logRequests);
  }

  private void setupJoinInputs(List<LogRequest> logRequests) {
    user = LogRequestFactory.pushDownToUsers(logRequests);
    view = LogRequestFactory.pushDownToViews(logRequests);
    deliveryLog = LogRequestFactory.pushDownToDeliveryLogs(logRequests);
    impression = LogRequestFactory.pushDownToImpressions(logRequests);
    action = LogRequestFactory.pushDownToActions(logRequests);

    // TODO - enable tests with Kafka.
    // https://github.com/apache/flink/blob/59905140b7d6a31f8c07fc8151f33f978e475bae/flink-connectors/flink-connector-kafka/src/test/java/org/apache/flink/streaming/connectors/kafka/table/UpsertKafkaTableITCase.java
  }

  private void executeJoinEvents() throws Exception {
    executeJoinEvents(job, user, view, deliveryLog, impression, action, contentDb);
  }

  // TODO - move to another file.

  // Helper method that sets operator uids to match the production inputs.
  private void executeJoinEvents(
      FlatOutputJob job,
      List<User> users,
      List<View> views,
      List<DeliveryLog> deliveryLogs,
      List<Impression> impressions,
      List<Action> actions,
      ContentDB contentDb)
      throws Exception {

    // For some reason, the original Lists are not serializable.
    users = ImmutableList.copyOf(users);
    views = ImmutableList.copyOf(views);
    deliveryLogs = ImmutableList.copyOf(deliveryLogs);
    impressions = ImmutableList.copyOf(impressions);
    actions = ImmutableList.copyOf(actions);

    job.contentApiSegment.contentApiLoader = new MockContentFieldLookup(contentDb);
    job.executeJoinEvents(
        fromCollectionSource(
            env,
            "view",
            new CollectionSource<>(views, e -> e.getTiming().getEventApiTimestamp()),
            TypeInformation.of(View.class),
            View::getTiming),
        fromCollectionSource(
            env,
            "delivery-log",
            new CollectionSource<>(
                deliveryLogs, d -> d.getRequest().getTiming().getEventApiTimestamp()),
            TypeInformation.of(DeliveryLog.class),
            d -> d.getRequest().getTiming()),
        fromCollectionSource(
            env,
            "impression",
            new CollectionSource<>(impressions, e -> e.getTiming().getEventApiTimestamp()),
            TypeInformation.of(Impression.class),
            Impression::getTiming),
        fromCollectionSource(
            env,
            "action",
            new CollectionSource<>(actions, e -> e.getTiming().getEventApiTimestamp()),
            TypeInformation.of(Action.class),
            Action::getTiming));
  }

  /** Looks up other content IDs in the mock Content DB. */
  public static class MockContentFieldLookup implements ContentAPILoader {
    private static final long serialVersionUID = 1234567L;
    private final ContentDB contentDb;

    MockContentFieldLookup(ContentDB contentDb) {
      this.contentDb = contentDb;
    }

    @Override
    public Map<String, Map<String, Object>> apply(Collection<String> contentId) {
      return contentId.stream()
          .collect(
              Collector.of(
                  ImmutableMap.Builder<String, Map<String, Object>>::new,
                  (m, c) -> m.put(c, getSingleContentFields(c)),
                  (m1, m2) -> m1.putAll(m2.build()),
                  ImmutableMap.Builder::build));
    }

    private Map<String, Object> getSingleContentFields(String contentId) {
      Content content = contentDb.getContent(contentId);
      Preconditions.checkState(content != null, "Content should exist for %s", contentId);
      Map<String, Object> otherContentIds =
          content.requestFields().entrySet().stream()
              // Do not include the contentId field again.
              .filter(
                  entry ->
                      matchingMockKey(entry.getKey())
                          && !content.contentId().equals(entry.getValue()))
              .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()))
              .collect(
                  Collector.of(
                      ImmutableMap.Builder<String, Object>::new,
                      (m, e) -> m.put(e.getKey(), e.getValue()),
                      (m1, m2) -> m1.putAll(m2.build()),
                      ImmutableMap.Builder::build));
      return otherContentIds;
    }

    private boolean matchingMockKey(String key) {
      return key.equals(ContentType.ITEM.id)
          || key.equals(ContentType.STORE.id)
          || key.equals(ContentType.PROMOTION.id);
    }
  }
}
