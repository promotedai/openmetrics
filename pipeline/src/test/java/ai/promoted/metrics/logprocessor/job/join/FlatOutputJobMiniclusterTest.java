package ai.promoted.metrics.logprocessor.job.join;

import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.assertAvroFiles;
import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.getNestedField;
import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.loadAvroRecords;
import static ai.promoted.metrics.logprocessor.common.testing.AvroProtoUtils.toFixedAvroGenericRecord;
import static ai.promoted.metrics.logprocessor.common.testing.AvroProtoUtils.toFixedAvroGenericRecords;
import static ai.promoted.metrics.logprocessor.common.testing.FileAsserts.assertPartFilesExist;
import static ai.promoted.metrics.logprocessor.common.util.TableUtil.getLabeledDatabaseName;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory.DetailLevel;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestIterator;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestIteratorOptions;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.Content;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDB;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDBFactory;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentType;
import ai.promoted.metrics.logprocessor.common.functions.UserInfoUtil;
import ai.promoted.metrics.logprocessor.common.functions.content.common.ContentQueryClient;
import ai.promoted.metrics.logprocessor.common.functions.content.common.ContentQueryParams;
import ai.promoted.metrics.logprocessor.common.functions.inferred.AttributionModel;
import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobMiniclusterTest;
import ai.promoted.metrics.logprocessor.common.testing.AvroAsserts;
import ai.promoted.metrics.logprocessor.common.testing.MiniClusterExtension;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.metrics.logprocessor.common.util.TrackingUtil;
import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.CurrencyCode;
import ai.promoted.proto.common.Money;
import ai.promoted.proto.common.Properties;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.delivery.DeliveryExecution;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.ExecutionServer;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.PagingInfo;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.Attribution;
import ai.promoted.proto.event.Cart;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.HiddenApiRequest;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyAttributedAction;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.TinyTouchpoint;
import ai.promoted.proto.event.Touchpoint;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
    EnvironmentSettings writerTableSettings =
        EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamTableEnvironment writerTableEnv = StreamTableEnvironment.create(env, writerTableSettings);
    job.setTableEnv(writerTableEnv);
    job.maxParallelism = 1;
    // TODO(PRO-813): enable synthetic user support in tests.

    timeMillis = 1601596151000L;
    dt = "2020-10-01";
    hour = "23";
    detailLevel = LogRequestFactory.DetailLevel.PARTIAL;

    expectedEtlDirs =
        Sets.newHashSet(
            "attributed_action",
            "flat_response_insertion",
            "joined_impression",
            "tiny_action_path",
            "tiny_attributed_action",
            "tiny_joined_impression");
    expectedDroppedDirs = Sets.newHashSet();
    expectedDebugDirs =
        Sets.newHashSet("debug_rhs_tiny_action", "debug_partial_response_insertion");
    expectedLateDirs = Sets.newHashSet();
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
    TrackingUtil.processingTimeSupplier = () -> 1L;
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
            .setShadowTrafficRate(0.0f)
            .setWriteLogUserId(true)
            .setWriteAnonUserId(true));

    assertEquals(1, deliveryLog.size());
    assertEquals(1, impression.size());
    assertEquals(1, action.size());

    // Change the tempDir to point to the blue child directory.
    tempDir = new File(tempDir, "blue");

    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    assertSideOutputCount();
    // CombinedDeliveryLog delays the Request by the window.
    JoinedImpression.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    if (detailLevel == DetailLevel.FULL) {
      // Populated by the PopulatePagingId operator.  Request is different for the FULL case.
      joinedImpressionBuilder
          .getResponseBuilder()
          .getPagingInfoBuilder()
          .setPagingId("BO5AW6Bwan2pK2OAHioS5A==");
    }

    expectedEtlDirs.add("flat_actioned_response_insertion");
    assertJoinedOneFullImpressionAndAction(joinedImpressionBuilder);
  }

  @Test
  void debugIds() throws Exception {
    this.detailLevel = DetailLevel.PARTIAL;
    this.timeMillis = 1601596151000L;
    this.dt = "2020-10-01";
    this.hour = "23";

    job.jobLabel = "blue";
    job.debugImpressionIds = ImmutableSet.of("55555555-5555-5555-0000-000000000001");
    job.debugActionIds = ImmutableSet.of("66666666-6666-6666-0000-000000000001");
    execute(
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1)
            .setMiniSdkRate(0.0f)
            .setShadowTrafficRate(0.0f)
            .setWriteLogUserId(true)
            .setWriteAnonUserId(true));

    assertEquals(1, deliveryLog.size());
    assertEquals(1, impression.size());
    assertEquals(1, action.size());

    // Change the tempDir to point to the blue child directory.
    tempDir = new File(tempDir, "blue");

    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    assertSideOutputCount();
    // CombinedDeliveryLog delays the Request by the window.
    JoinedImpression.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    if (detailLevel == DetailLevel.FULL) {
      // Populated by the PopulatePagingId operator.  Request is different for the FULL case.
      joinedImpressionBuilder
          .getResponseBuilder()
          .getPagingInfoBuilder()
          .setPagingId("BO5AW6Bwan2pK2OAHioS5A==");
    }

    expectedEtlDirs.add("flat_actioned_response_insertion");
    assertJoinedOneFullImpressionAndAction(joinedImpressionBuilder);
  }

  @Test
  void redundantInsertion() throws Exception {
    this.detailLevel = LogRequestFactory.DetailLevel.PARTIAL;
    this.timeMillis = 1601603351000L;
    this.dt = "2020-10-02";
    this.hour = "01";

    job.jobLabel = "blue";
    execute(
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1)
            .setMiniSdkRate(0.0f)
            .setShadowTrafficRate(0.0f)
            .setWriteLogUserId(true)
            .setWriteAnonUserId(true)
            .setRedundantInsertionRate(1.0f)
            .setRedundantImpressionRate(2.0f)
            .setMaxRedundantImpressionsPerDeliveryLog(3));

    assertEquals(2, deliveryLog.size());
    assertEquals(6, impression.size());
    assertEquals(2, action.size());

    // Change the tempDir to point to the blue child directory.
    tempDir = new File(tempDir, "blue");

    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    expectedDroppedDirs.add("dropped_redundant_impression");
    expectedDroppedDirs.add("dropped_redundant_action");
    assertSideOutputCount();

    // Assert there is only one record in joined_impression and joined_action.
    // This will help debug the test in case the specific assert starts failing.
    String joinedImpressionPath = String.format("etl/joined_impression/dt=%s/hour=%s", dt, hour);
    File[] joinedImpressionFiles = assertPartFilesExist(new File(tempDir, joinedImpressionPath));
    ImmutableSet<GenericRecord> actualJoinedImpressions = loadAvroRecords(joinedImpressionFiles);
    assertThat(actualJoinedImpressions).hasSize(1);

    String attributedActionPath = String.format("etl/attributed_action/dt=%s/hour=%s", dt, hour);
    File[] attributedActionFiles = assertPartFilesExist(new File(tempDir, attributedActionPath));
    assertThat(loadAvroRecords(attributedActionFiles)).hasSize(2);

    // CombinedDeliveryLog delays the Request by the window.
    JoinedImpression.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    expectedEtlDirs.add("flat_actioned_response_insertion");
    assertJoinedOneFullImpressionAndAction(joinedImpressionBuilder, true);
  }

  @Test
  void redundantImpression() throws Exception {
    this.detailLevel = LogRequestFactory.DetailLevel.PARTIAL;
    this.timeMillis = 1601603351000L;
    this.dt = "2020-10-02";
    this.hour = "01";

    job.jobLabel = "blue";
    execute(
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1)
            .setMiniSdkRate(0.0f)
            .setShadowTrafficRate(0.0f)
            .setWriteLogUserId(true)
            .setWriteAnonUserId(true)
            .setRedundantImpressionRate(2.0f)
            .setMaxRedundantImpressionsPerDeliveryLog(3));

    assertEquals(1, deliveryLog.size());
    assertEquals(3, impression.size());
    assertEquals(1, action.size());

    // Change the tempDir to point to the blue child directory.
    tempDir = new File(tempDir, "blue");

    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    expectedDroppedDirs.add("dropped_redundant_impression");
    assertSideOutputCount();

    // Assert there is only one record in joined_impression and joined_action.
    // This will help debug the test in case the specific assert starts failing.
    String joinedImpressionPath = String.format("etl/joined_impression/dt=%s/hour=%s", dt, hour);
    File[] joinedImpressionFiles = assertPartFilesExist(new File(tempDir, joinedImpressionPath));
    assertThat(loadAvroRecords(joinedImpressionFiles)).hasSize(1);

    String attributedActionPath = String.format("etl/attributed_action/dt=%s/hour=%s", dt, hour);
    File[] attributedActionFiles = assertPartFilesExist(new File(tempDir, attributedActionPath));
    assertThat(loadAvroRecords(attributedActionFiles)).hasSize(2);

    // CombinedDeliveryLog delays the Request by the window.
    JoinedImpression.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();

    expectedEtlDirs.add("flat_actioned_response_insertion");
    assertJoinedOneFullImpressionAndAction(joinedImpressionBuilder);
  }

  // Verify that there are 2 NAVIGATES
  @Test
  void reduceIsImpressionAction() throws Exception {
    this.detailLevel = LogRequestFactory.DetailLevel.PARTIAL;
    this.timeMillis = 1601603351000L;
    this.dt = "2020-10-02";
    this.hour = "01";

    job.jobLabel = "blue";
    LogRequestIteratorOptions options1 =
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1, "0000", "0000")
            .setMiniSdkRate(0.0f)
            .setShadowTrafficRate(0.0f)
            .setWriteLogUserId(true)
            .setWriteAnonUserId(true)
            .build();
    // Create additional actions for the same user that look similar but are different.
    LogRequestIteratorOptions options2 =
        LogRequestFactory.createLogRequestOptionsBuilder(
                timeMillis + 1000, detailLevel, 1, "0000", "0001")
            .setMiniSdkRate(0.0f)
            .setShadowTrafficRate(0.0f)
            .setWriteLogUserId(true)
            .setWriteAnonUserId(true)
            .build();
    contentDb =
        ContentDBFactory.create(LogRequestIterator.PLATFORM_ID, options1.contentDBFactoryOptions());
    ImmutableList.Builder<LogRequest> logRequestsBuilder = ImmutableList.builder();
    logRequestsBuilder.addAll(new LogRequestIterator(options1, contentDb));
    logRequestsBuilder.addAll(new LogRequestIterator(options2, contentDb));
    setupJoinInputs(logRequestsBuilder.build());

    executeJoinEvents();
    waitForDone(env.execute("join-event"));

    assertEquals(2, deliveryLog.size());
    assertEquals(2, impression.size());
    assertEquals(2, action.size());

    // Change the tempDir to point to the blue child directory.
    tempDir = new File(tempDir, "blue");

    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    expectedDroppedDirs.add("dropped_redundant_impression");
    expectedDroppedDirs.add("dropped_redundant_action");
    assertSideOutputCount();

    // The goal of this test is to see that we haven't reduced the navigates.
    assertThat(action.get(0).getContentId()).isNotEmpty();
    assertThat(action.get(0).getContentId()).isEqualTo(action.get(1).getContentId());
    String attributedActionPath = String.format("etl/attributed_action/dt=%s/hour=%s", dt, hour);
    File[] attributedActionFiles = assertPartFilesExist(new File(tempDir, attributedActionPath));
    assertThat(loadAvroRecords(attributedActionFiles)).hasSize(2);
  }

  // Temporary sanity check to make sure that the job joins based on anonUserId.
  @Test
  void noLogUserId() throws Exception {
    this.detailLevel = DetailLevel.PARTIAL;
    this.timeMillis = 1601596151000L;
    this.dt = "2020-10-01";
    this.hour = "23";

    job.jobLabel = "blue";
    execute(
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1)
            .setMiniSdkRate(0.0f)
            .setShadowTrafficRate(0.0f)
            .setWriteLogUserId(false)
            .setWriteAnonUserId(true));

    assertEquals(1, deliveryLog.size());
    assertEquals(1, impression.size());
    assertEquals(1, action.size());

    // Change the tempDir to point to the blue child directory.
    tempDir = new File(tempDir, "blue");

    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    assertSideOutputCount();
    // CombinedDeliveryLog delays the Request by the window.
    JoinedImpression.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    joinedImpressionBuilder.getIdsBuilder().clearLogUserId();
    // Populated by the PopulatePagingId operator.  Request is different for the missing logUserId.
    joinedImpressionBuilder
        .getResponseBuilder()
        .getPagingInfoBuilder()
        .setPagingId("4cgBt/xha2HhJJiHVFnDgw==");

    expectedEtlDirs.add("flat_actioned_response_insertion");
    assertJoinedOneFullImpressionAndAction(joinedImpressionBuilder);
  }

  @Test
  void testPaimonOutput() throws Exception {
    String defaultDatabase = "flat_db";
    job.contentApiSegment.contentIdFieldKeys = ImmutableList.of(ContentType.STORE.id);
    job.writePaimonTables = true;
    job.databaseName = defaultDatabase;
    job.sideDatabaseName = "flat_db_side";
    job.paimonSegment.paimonBasePath = tempDir.getAbsolutePath() + "/paimon-test/";
    job.jobLabel = "blue";
    setupJoinInputs(LogRequestFactory.createStoreToItemBuilder().setWriteAnonUserId(true));
    executeJoinEvents();
    job.prepareToExecute();
    env.getCheckpointConfig().enableUnalignedCheckpoints(false);
    waitForDone(env.execute("join-event"));

    // Validate
    EnvironmentSettings readerTableSettings =
        EnvironmentSettings.newInstance().inBatchMode().build();
    StreamTableEnvironment readerTableEnv = StreamTableEnvironment.create(env, readerTableSettings);
    String catalogPath = new File(tempDir, "/paimon-test/").getAbsolutePath();
    String createCatalogSql =
        String.format(
            "CREATE CATALOG paimon WITH ('warehouse'='%s', 'type'='paimon', 'default-database'='%s')",
            catalogPath, getLabeledDatabaseName(defaultDatabase, "blue"));
    readerTableEnv.executeSql(createCatalogSql);
    readerTableEnv.executeSql("use catalog paimon");
    List<String> expectedDatabaseList =
        List.of("flat_db_blue", "flat_db_side_blue", "default_blue");
    List<String> databaseList =
        ImmutableList.copyOf(readerTableEnv.executeSql("show databases").collect()).stream()
            .map(row -> row.getField(0).toString())
            .collect(Collectors.toList());
    assertThat(databaseList).containsExactlyElementsIn(expectedDatabaseList);
    readerTableEnv.executeSql("use flat_db_blue");
    List<String> expectedFlatTables =
        List.of(
            "attributed_action",
            "flat_response_insertion",
            "joined_impression",
            "tiny_action_path",
            "tiny_attributed_action",
            "tiny_joined_impression");
    List<String> flatTables =
        ImmutableList.copyOf(readerTableEnv.executeSql("show tables").collect()).stream()
            .map(row -> row.getField(0).toString())
            .collect(Collectors.toList());
    assertThat(flatTables).containsExactlyElementsIn(expectedFlatTables);

    readerTableEnv.executeSql("use flat_db_side_blue");
    List<String> sideTables =
        ImmutableList.copyOf(readerTableEnv.executeSql("show tables").collect()).stream()
            .map(row -> row.getField(0).toString())
            .collect(Collectors.toList());
    List<String> expectedSideTables =
        List.of(
            "debug_partial_response_insertion",
            "debug_rhs_tiny_action",
            "dropped_delivery_log_is_bot",
            "dropped_delivery_log_should_not_join",
            "dropped_ignore_usage_attributed_action",
            "dropped_ignore_usage_flat_response_insertion",
            "dropped_ignore_usage_joined_impression",
            "dropped_impression_actions",
            "dropped_insertion_impressions",
            "dropped_merge_action_details",
            "dropped_merge_impression_details",
            "dropped_redundant_action",
            "dropped_redundant_impression",
            "mismatch_error");
    assertThat(sideTables).containsExactlyElementsIn(expectedSideTables);
    List<String> joinedImpressionIdsList = new ArrayList<>();
    readerTableEnv
        .executeSql("SELECT ids FROM paimon.flat_db_blue.joined_impression")
        .collect()
        .forEachRemaining(row -> joinedImpressionIdsList.add(row.toString()));
    assertThat(joinedImpressionIdsList)
        .containsExactly(
            "+I[+I[1, null, 00000000-0000-0000-0000-000000000001, 00000000-0000-0000-0000-000000000001, null, null, null, 33333333-3333-3333-0000-000000000001, 44444444-4444-4444-0000-000000000001, 55555555-5555-5555-0000-000000000001]]");
  }

  @DisplayName("Store page to item page (no impression) to checkout to purchase")
  @Test
  void storeToItem() throws Exception {
    job.contentApiSegment.contentIdFieldKeys = ImmutableList.of(ContentType.STORE.id);

    setupJoinInputs(
        LogRequestFactory.createStoreToItemBuilder()
            .setWriteAnonUserId(true)
            .setWriteLogUserId(true));

    // TODO - change fake data generator to support this case.
    assertEquals(1, impression.size());
    assertEquals(4, action.size());

    executeJoinEvents();

    waitForDone(env.execute("join-event"));

    expectedEtlDirs.add("flat_actioned_response_insertion");
    assertEtlOutputCount();
    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    assertSideOutputCount();

    // CombinedDeliveryLog delays the Request by the window.
    JoinedImpression.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    joinedImpressionBuilder.getIdsBuilder().clearSessionId();
    joinedImpressionBuilder.getIdsBuilder().clearViewId();
    assertJoinedImpression(joinedImpressionBuilder.clone());

    // Assert that we have 3 Actions.
    String actionPath = String.format("etl/attributed_action/dt=%s/hour=%s", dt, hour);
    File[] actionFiles = assertPartFilesExist(new File(tempDir, actionPath));
    Set<GenericRecord> actualAttributedActions = loadAvroRecords(actionFiles);
    assertEquals(8, actualAttributedActions.size());

    List<GenericRecord> lastTouchpointAction =
        actualAttributedActions.stream()
            .filter(action -> ((Long) getNestedField(action, "attribution", "model_id")) == 1L)
            .collect(Collectors.toList());

    Map<String, GenericRecord> actionIdToAction =
        lastTouchpointAction.stream()
            .collect(
                Collectors.toMap(a -> (String) getNestedField(a, "action", "action_id"), a -> a));

    assertEquals(
        ImmutableSet.of(
            "66666666-6666-6666-0000-000000000001",
            "66666666-6666-6666-0000-000000000003",
            "66666666-6666-6666-0000-000000000004",
            "66666666-6666-6666-0000-000000000005"),
        actionIdToAction.keySet());

    Action.Builder actionBuilder = action.get(0).toBuilder();
    actionBuilder.getTimingBuilder().setProcessingTimestamp(1L);
    AttributedAction.Builder expectedNavigate =
        AttributedAction.newBuilder()
            .setAction(actionBuilder)
            .setAttribution(
                Attribution.newBuilder()
                    .setModelId(AttributionModel.LATEST.id)
                    .setCreditMillis(1000))
            .setTouchpoint(
                Touchpoint.newBuilder().setJoinedImpression(joinedImpressionBuilder.clone()));
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedNavigate.clone().build()),
        actionIdToAction.get("66666666-6666-6666-0000-000000000001"),
        "navigate");
    AttributedAction.Builder expectedAddToCart = expectedNavigate.clone();
    expectedAddToCart
        .getActionBuilder()
        .setContentId("i-1-1")
        .setActionType(ActionType.ADD_TO_CART)
        .setActionId("66666666-6666-6666-0000-000000000003");
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedAddToCart.build()),
        actionIdToAction.get("66666666-6666-6666-0000-000000000003"),
        "addToCart");
    AttributedAction.Builder expectedCheckout = expectedNavigate.clone();
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
    AttributedAction.Builder expectedPurchase = expectedNavigate.clone();
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
    job.contentApiSegment.contentIdFieldKeys = ImmutableList.of(ContentType.STORE.id);

    LogRequestIteratorOptions options =
        LogRequestFactory.createItemShoppingCartBuilder()
            .setWriteLogUserId(true)
            .setWriteAnonUserId(true)
            .build();
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

    expectedDroppedDirs.add("dropped_redundant_action");
    expectedEtlDirs.add("flat_actioned_response_insertion");
    assertEtlOutputCount();
    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    assertSideOutputCount();

    String impressionPath = String.format("etl/joined_impression/dt=%s/hour=%s", dt, hour);
    File[] impressionFiles = assertPartFilesExist(new File(tempDir, impressionPath));
    Set<GenericRecord> actualImpressions = loadAvroRecords(impressionFiles);
    assertEquals(16, actualImpressions.size());

    // Assert that we have 3 Actions.
    String attributedActionPath = String.format("etl/attributed_action/dt=%s/hour=%s", dt, hour);
    File[] attributedActionFiles = assertPartFilesExist(new File(tempDir, attributedActionPath));
    Set<GenericRecord> actualAttributedActions = loadAvroRecords(attributedActionFiles);
    assertEquals(56, actualAttributedActions.size());

    List<GenericRecord> lastTouchpointAction =
        actualAttributedActions.stream()
            .filter(action -> ((Long) getNestedField(action, "attribution", "model_id")) == 1L)
            .collect(Collectors.toList());

    assertEquals(28, lastTouchpointAction.size());

    Map<String, Long> actionTypeToCount =
        lastTouchpointAction.stream()
            .collect(
                Collectors.groupingBy(
                    action -> getNestedField(action, "action", "action_type").toString(),
                    Collectors.counting()));
    assertEquals(
        ImmutableMap.of("ADD_TO_CART", 4L, "NAVIGATE", 16L, "CHECKOUT", 4L, "PURCHASE", 4L),
        actionTypeToCount);

    Set<String> checkoutIds =
        lastTouchpointAction.stream()
            .filter(a -> getNestedField(a, "action", "action_type").toString().equals("CHECKOUT"))
            .map(a -> (String) getNestedField(a, "action", "action_id"))
            .collect(Collectors.toSet());

    assertEquals(ImmutableSet.of("66666666-6666-6666-0000-000000000019"), checkoutIds);

    // CombinedDeliveryLog delays the Request by the window.
    JoinedImpression.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    joinedImpressionBuilder
        .getIdsBuilder()
        .setAnonUserId("00000000-0000-0000-0000-000000000001")
        .setLogUserId("00000000-0000-0000-0000-000000000001")
        .clearSessionId()
        .setRequestId("33333333-3333-3333-0000-000000000002")
        .setImpressionId("55555555-5555-5555-0000-000000000005")
        .setInsertionId("44444444-4444-4444-0000-000000000005")
        .setViewId("22222222-2222-2222-0000-000000000002");
    joinedImpressionBuilder.setRequestInsertion(
        createExpectedItemRequestInsertion("s-1-1", "i-1-1"));
    joinedImpressionBuilder
        .getRequestBuilder()
        .setClientRequestId("client-33333333-3333-3333-0000-000000000002");
    joinedImpressionBuilder.getResponseInsertionBuilder().setContentId("i-1-1");
    joinedImpressionBuilder.getImpressionBuilder().setContentId("i-1-1");

    Action.Builder actionBuilder = action.get(0).toBuilder().clearImpressionId();
    actionBuilder.getTimingBuilder().setProcessingTimestamp(1L);
    AttributedAction.Builder expectedNavigate =
        AttributedAction.newBuilder()
            // TODO - why is impressionId cleared?  It should get cleared on flat_response_insertion
            // but it's fine to leave on AttributedAction.
            .setAction(actionBuilder)
            .setAttribution(
                Attribution.newBuilder()
                    .setModelId(AttributionModel.LATEST.id)
                    .setCreditMillis(1000))
            .setTouchpoint(
                Touchpoint.newBuilder().setJoinedImpression(joinedImpressionBuilder.clone()));

    AttributedAction.Builder expectedCheckout1 = expectedNavigate.clone();
    expectedCheckout1
        .getActionBuilder()
        // Action.content_id is the original content_id on the raw logged Action.
        .setContentId("i-1-1")
        .setActionType(ActionType.CHECKOUT)
        .setActionId("66666666-6666-6666-0000-000000000019")
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
        lastTouchpointAction.stream()
            .filter(
                a ->
                    getNestedField(a, "action", "action_id")
                        .equals("66666666-6666-6666-0000-000000000019"))
            .collect(
                Collectors.toMap(
                    a ->
                        (String)
                            getNestedField(
                                a,
                                "touchpoint",
                                "joined_impression",
                                "response_insertion",
                                "content_id"),
                    a -> a));

    assertEquals(4, contentIdToCheckouts.size());
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedCheckout1.clone().build()),
        contentIdToCheckouts.get("i-1-1"),
        "checkout-1");

    AttributedAction.Builder expectedCheckout181Builder = expectedCheckout1.clone();
    JoinedImpression.Builder expectedJoinedImpression181Builder =
        expectedCheckout181Builder.getTouchpointBuilder().getJoinedImpressionBuilder();
    expectedJoinedImpression181Builder
        .getIdsBuilder()
        .setRequestId("33333333-3333-3333-0000-000000000003")
        .setImpressionId("55555555-5555-5555-0000-000000000009")
        .setInsertionId("44444444-4444-4444-0000-000000000009")
        .setViewId("22222222-2222-2222-0000-000000000003");
    expectedJoinedImpression181Builder
        .getRequestBuilder()
        .setClientRequestId("client-33333333-3333-3333-0000-000000000003");
    expectedJoinedImpression181Builder.setRequestInsertion(
        createExpectedItemRequestInsertion("s-1-10", "i-1-181"));
    expectedJoinedImpression181Builder
        .getApiExecutionInsertionBuilder()
        .getFeatureStageBuilder()
        .getFeaturesBuilder()
        .putNumeric(100, 0.5f);
    expectedJoinedImpression181Builder.getResponseInsertionBuilder().setContentId("i-1-181");
    expectedJoinedImpression181Builder.getImpressionBuilder().setContentId("i-1-181");
    expectedCheckout181Builder
        .getActionBuilder()
        .getSingleCartContentBuilder()
        .setContentId("i-1-181")
        .setQuantity(4)
        .setPricePerUnit(
            Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(4000000L));

    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedCheckout181Builder.clone().build()),
        contentIdToCheckouts.get("i-1-181"),
        "checkout-181");

    AttributedAction.Builder expectedCheckout201Builder = expectedCheckout1.clone();
    JoinedImpression.Builder expectedJoinedImpression201Builder =
        expectedCheckout201Builder.getTouchpointBuilder().getJoinedImpressionBuilder();
    expectedJoinedImpression201Builder
        .getIdsBuilder()
        .setRequestId("33333333-3333-3333-0000-000000000004")
        .setImpressionId("55555555-5555-5555-0000-00000000000d")
        .setInsertionId("44444444-4444-4444-0000-00000000000d")
        .setViewId("22222222-2222-2222-0000-000000000004");
    expectedJoinedImpression201Builder
        .getRequestBuilder()
        .setClientRequestId("client-33333333-3333-3333-0000-000000000004");
    expectedJoinedImpression201Builder.setRequestInsertion(
        createExpectedItemRequestInsertion("s-1-11", "i-1-201"));
    expectedJoinedImpression201Builder
        .getApiExecutionInsertionBuilder()
        .getFeatureStageBuilder()
        .getFeaturesBuilder()
        .putNumeric(100, 0.3f);
    expectedJoinedImpression201Builder.getResponseInsertionBuilder().setContentId("i-1-201");
    expectedJoinedImpression201Builder.getImpressionBuilder().setContentId("i-1-201");
    expectedCheckout201Builder
        .getActionBuilder()
        .getSingleCartContentBuilder()
        .setContentId("i-1-201")
        .setQuantity(3)
        .setPricePerUnit(
            Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(2000000L));

    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedCheckout201Builder.clone().build()),
        contentIdToCheckouts.get("i-1-201"),
        "checkout-201");

    AttributedAction.Builder expectedCheckout221Builder = expectedCheckout1.clone();
    JoinedImpression.Builder expectedJoinedImpression221Builder =
        expectedCheckout221Builder.getTouchpointBuilder().getJoinedImpressionBuilder();
    expectedJoinedImpression221Builder
        .getIdsBuilder()
        .setRequestId("33333333-3333-3333-0000-000000000005")
        .setImpressionId("55555555-5555-5555-0000-000000000011")
        .setInsertionId("44444444-4444-4444-0000-000000000011")
        .setViewId("22222222-2222-2222-0000-000000000005");
    expectedJoinedImpression221Builder
        .getRequestBuilder()
        .setClientRequestId("client-33333333-3333-3333-0000-000000000005");
    expectedJoinedImpression221Builder.setRequestInsertion(
        createExpectedItemRequestInsertion("s-1-12", "i-1-221"));
    expectedJoinedImpression221Builder.getResponseInsertionBuilder().setContentId("i-1-221");
    expectedJoinedImpression221Builder.getImpressionBuilder().setContentId("i-1-221");
    expectedCheckout221Builder
        .getActionBuilder()
        .getSingleCartContentBuilder()
        .setContentId("i-1-221")
        .setQuantity(1)
        .setPricePerUnit(
            Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(4000000L));

    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedCheckout221Builder.clone().build()),
        contentIdToCheckouts.get("i-1-221"),
        "checkout-221");

    // Assert purchases.
    Set<String> purchaseIds =
        lastTouchpointAction.stream()
            .filter(a -> getNestedField(a, "action", "action_type").toString().equals("PURCHASE"))
            .map(a -> (String) getNestedField(a, "action", "action_id"))
            .collect(Collectors.toSet());

    assertEquals(ImmutableSet.of("66666666-6666-6666-0000-00000000001d"), purchaseIds);

    Map<String, GenericRecord> contentIdToPurchases =
        lastTouchpointAction.stream()
            .filter(
                a ->
                    getNestedField(a, "action", "action_id")
                        .equals("66666666-6666-6666-0000-00000000001d"))
            .collect(
                Collectors.toMap(
                    a ->
                        (String)
                            getNestedField(
                                a,
                                "touchpoint",
                                "joined_impression",
                                "response_insertion",
                                "content_id"),
                    a -> a));
    assertEquals(4, contentIdToPurchases.size());

    AttributedAction.Builder expectedPurchase1 = expectedCheckout1.clone();
    expectedPurchase1.getActionBuilder().setActionType(ActionType.PURCHASE);
    expectedPurchase1.getActionBuilder().setActionId("66666666-6666-6666-0000-00000000001d");
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedPurchase1.build()),
        contentIdToPurchases.get("i-1-1"),
        "purchase-1");

    AttributedAction.Builder expectedPurchase181 = expectedCheckout181Builder.clone();
    expectedPurchase181.getActionBuilder().setActionType(ActionType.PURCHASE);
    expectedPurchase181.getActionBuilder().setActionId("66666666-6666-6666-0000-00000000001d");
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedPurchase181.build()),
        contentIdToPurchases.get("i-1-181"),
        "purchase-181");

    AttributedAction.Builder expectedPurchase201 = expectedCheckout201Builder.clone();
    expectedPurchase201.getActionBuilder().setActionType(ActionType.PURCHASE);
    expectedPurchase201.getActionBuilder().setActionId("66666666-6666-6666-0000-00000000001d");
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedPurchase201.build()),
        contentIdToPurchases.get("i-1-201"),
        "purchase-201");

    AttributedAction.Builder expectedPurchase221 = expectedCheckout221Builder.clone();
    expectedPurchase221.getActionBuilder().setActionType(ActionType.PURCHASE);
    expectedPurchase221.getActionBuilder().setActionId("66666666-6666-6666-0000-00000000001d");
    AvroAsserts.assertGenericRecordEquals(
        toFixedAvroGenericRecord(expectedPurchase221.build()),
        contentIdToPurchases.get("i-1-221"),
        "purchase-221");
  }

  @DisplayName("Shopping cart > store insertion")
  @Test
  void storeToItem_cart_storeInsertion() throws Exception {
    job.contentApiSegment.contentIdFieldKeys = ImmutableList.of(ContentType.STORE.id);

    LogRequestIteratorOptions options =
        LogRequestFactory.createStoreInsertionItemShoppingCartBuilder()
            .setWriteAnonUserId(true)
            .setWriteLogUserId(true)
            .build();
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

    expectedDroppedDirs.add("dropped_redundant_action");
    expectedEtlDirs.add("flat_actioned_response_insertion");
    assertEtlOutputCount();
    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    assertSideOutputCount();

    String impressionPath = String.format("etl/joined_impression/dt=%s/hour=%s", dt, hour);
    File[] impressionFiles = assertPartFilesExist(new File(tempDir, impressionPath));
    Set<GenericRecord> actualImpressions = loadAvroRecords(impressionFiles);
    assertEquals(4, actualImpressions.size());

    // Assert that we have 3 Actions.
    String actionPath = String.format("etl/attributed_action/dt=%s/hour=%s", dt, hour);
    File[] actionFiles = assertPartFilesExist(new File(tempDir, actionPath));
    Set<GenericRecord> actualAttributedActions = loadAvroRecords(actionFiles);
    assertEquals(32, actualAttributedActions.size());

    List<GenericRecord> lastTouchpointAction =
        actualAttributedActions.stream()
            .filter(action -> ((Long) getNestedField(action, "attribution", "model_id")) == 1L)
            .collect(Collectors.toList());

    assertEquals(16, lastTouchpointAction.size());

    Map<String, Long> actionTypeToCount =
        lastTouchpointAction.stream()
            .collect(
                Collectors.groupingBy(
                    action -> getNestedField(action, "action", "action_type").toString(),
                    Collectors.counting()));

    // The raw Actions has 4x purchase that look very similar.  The carts are similar.  The main
    // difference is the main content_id is different.  The redundant action code will remove
    // the redundant actions.
    assertEquals(
        ImmutableMap.of("ADD_TO_CART", 4L, "NAVIGATE", 4L, "CHECKOUT", 4L, "PURCHASE", 4L),
        actionTypeToCount);

    Set<String> checkoutIds =
        lastTouchpointAction.stream()
            .filter(a -> getNestedField(a, "action", "action_type").toString().equals("CHECKOUT"))
            .map(a -> (String) getNestedField(a, "action", "action_id"))
            .collect(Collectors.toSet());

    assertEquals(ImmutableSet.of("66666666-6666-6666-0000-000000000019"), checkoutIds);

    Map<String, GenericRecord> contentIdToCheckouts =
        lastTouchpointAction.stream()
            .filter(
                a ->
                    getNestedField(a, "action", "action_id")
                        .equals("66666666-6666-6666-0000-000000000019"))
            .collect(
                Collectors.toMap(
                    a ->
                        (String)
                            getNestedField(
                                a,
                                "touchpoint",
                                "joined_impression",
                                "response_insertion",
                                "content_id"),
                    a -> a));
    assertEquals(4, contentIdToCheckouts.size());

    assertEquals(
        ImmutableSet.of("s-1-1", "s-1-11", "s-1-12", "s-1-10"), contentIdToCheckouts.keySet());

    // CombinedDeliveryLog delays the Request by the window.
    JoinedImpression.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    joinedImpressionBuilder
        .getIdsBuilder()
        .setAnonUserId("00000000-0000-0000-0000-000000000001")
        .setLogUserId("00000000-0000-0000-0000-000000000001")
        .clearSessionId()
        .setRequestId("33333333-3333-3333-0000-000000000001")
        .setImpressionId("55555555-5555-5555-0000-000000000001")
        .setInsertionId("44444444-4444-4444-0000-000000000001")
        .setViewId("22222222-2222-2222-0000-000000000001");
    joinedImpressionBuilder
        .getRequestBuilder()
        .setClientRequestId("client-33333333-3333-3333-0000-000000000001");
    joinedImpressionBuilder.setRequestInsertion(createExpectedStoreRequestInsertion("s-1-1"));
    joinedImpressionBuilder.getResponseInsertionBuilder().setContentId("s-1-1");
    joinedImpressionBuilder.getImpressionBuilder().setContentId("s-1-1");

    Action.Builder actionBuilder = action.get(0).toBuilder();
    actionBuilder.getTimingBuilder().setProcessingTimestamp(1L);
    AttributedAction.Builder expectedNavigate =
        AttributedAction.newBuilder()
            .setAction(actionBuilder)
            .setAttribution(
                Attribution.newBuilder()
                    .setModelId(AttributionModel.LATEST.id)
                    .setCreditMillis(1000))
            .setTouchpoint(
                Touchpoint.newBuilder().setJoinedImpression(joinedImpressionBuilder.clone()));

    AttributedAction.Builder expectedCheckout1 = expectedNavigate.clone();
    expectedCheckout1
        .getActionBuilder()
        // Action.content_id is the original content_id on the raw logged Action.
        .setContentId("i-1-1")
        .setActionType(ActionType.CHECKOUT)
        .setActionId("66666666-6666-6666-0000-000000000019")
        // clearRedundantFlatActionFields clears out redundant fields.
        .clearImpressionId()
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
    List<String> requestIdFieldPath =
        ImmutableList.of("touchpoint", "joined_impression", "ids", "request_id");
    List<String> insertionIdFieldPath =
        ImmutableList.of("touchpoint", "joined_impression", "ids", "insertion_id");
    List<String> impressionIdFieldPath =
        ImmutableList.of("touchpoint", "joined_impression", "ids", "impression_id");
    List<String> insertionContentIdFieldPath =
        ImmutableList.of("touchpoint", "joined_impression", "response_insertion", "content_id");
    List<String> impressionContentIdFieldPath =
        ImmutableList.of("touchpoint", "joined_impression", "impression", "content_id");
    List<String> actionSingleCartContentIdFieldPath =
        ImmutableList.of("action", "single_cart_content", "content_id");
    List<String> actionTypeFieldPath = ImmutableList.of("action", "action_type");

    GenericRecord actualCheckout10 = contentIdToCheckouts.get("s-1-10");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualCheckout10, requestIdFieldPath));
    assertEquals(
        "44444444-4444-4444-0000-000000000002",
        getNestedField(actualCheckout10, insertionIdFieldPath));
    assertEquals(
        "55555555-5555-5555-0000-000000000002",
        getNestedField(actualCheckout10, impressionIdFieldPath));
    assertEquals("s-1-10", getNestedField(actualCheckout10, insertionContentIdFieldPath));
    assertEquals("s-1-10", getNestedField(actualCheckout10, impressionContentIdFieldPath));
    assertEquals("i-1-181", getNestedField(actualCheckout10, actionSingleCartContentIdFieldPath));

    GenericRecord actualCheckout11 = contentIdToCheckouts.get("s-1-11");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualCheckout11, requestIdFieldPath));
    assertEquals(
        "44444444-4444-4444-0000-000000000003",
        getNestedField(actualCheckout11, insertionIdFieldPath));
    assertEquals(
        "55555555-5555-5555-0000-000000000003",
        getNestedField(actualCheckout11, impressionIdFieldPath));
    assertEquals("s-1-11", getNestedField(actualCheckout11, insertionContentIdFieldPath));
    assertEquals("s-1-11", getNestedField(actualCheckout11, impressionContentIdFieldPath));
    assertEquals("i-1-201", getNestedField(actualCheckout11, actionSingleCartContentIdFieldPath));

    GenericRecord actualCheckout12 = contentIdToCheckouts.get("s-1-12");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualCheckout12, requestIdFieldPath));
    assertEquals(
        "44444444-4444-4444-0000-000000000004",
        getNestedField(actualCheckout12, insertionIdFieldPath));
    assertEquals(
        "55555555-5555-5555-0000-000000000004",
        getNestedField(actualCheckout12, impressionIdFieldPath));
    assertEquals("s-1-12", getNestedField(actualCheckout12, insertionContentIdFieldPath));
    assertEquals("s-1-12", getNestedField(actualCheckout12, impressionContentIdFieldPath));
    assertEquals("i-1-221", getNestedField(actualCheckout12, actionSingleCartContentIdFieldPath));

    Set<String> purchaseIds =
        lastTouchpointAction.stream()
            .filter(a -> getNestedField(a, actionTypeFieldPath).toString().equals("PURCHASE"))
            .map(a -> (String) getNestedField(a, "action", "action_id"))
            .collect(Collectors.toSet());

    assertEquals(ImmutableSet.of("66666666-6666-6666-0000-00000000001d"), purchaseIds);

    Map<String, GenericRecord> contentIdToPurchases =
        lastTouchpointAction.stream()
            .filter(
                a ->
                    getNestedField(a, "action", "action_id")
                        .equals("66666666-6666-6666-0000-00000000001d"))
            .collect(
                Collectors.toMap(
                    a -> (String) getNestedField(a, insertionContentIdFieldPath), a -> a));
    assertEquals(4, contentIdToCheckouts.size());

    assertEquals(
        ImmutableSet.of("s-1-1", "s-1-11", "s-1-12", "s-1-10"), contentIdToPurchases.keySet());

    GenericRecord actualPurchase1 = contentIdToPurchases.get("s-1-1");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualPurchase1, requestIdFieldPath));
    assertEquals(
        "44444444-4444-4444-0000-000000000001",
        getNestedField(actualPurchase1, insertionIdFieldPath));
    assertEquals(
        "55555555-5555-5555-0000-000000000001",
        getNestedField(actualPurchase1, impressionIdFieldPath));
    assertEquals("s-1-1", getNestedField(actualPurchase1, insertionContentIdFieldPath));
    assertEquals("s-1-1", getNestedField(actualPurchase1, impressionContentIdFieldPath));
    assertEquals("PURCHASE", getNestedField(actualPurchase1, actionTypeFieldPath).toString());
    assertEquals("i-1-1", getNestedField(actualPurchase1, actionSingleCartContentIdFieldPath));

    GenericRecord actualPurchase10 = contentIdToPurchases.get("s-1-10");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualPurchase10, requestIdFieldPath));
    assertEquals(
        "44444444-4444-4444-0000-000000000002",
        getNestedField(actualPurchase10, insertionIdFieldPath));
    assertEquals(
        "55555555-5555-5555-0000-000000000002",
        getNestedField(actualPurchase10, impressionIdFieldPath));
    assertEquals("s-1-10", getNestedField(actualPurchase10, insertionContentIdFieldPath));
    assertEquals("s-1-10", getNestedField(actualPurchase10, impressionContentIdFieldPath));
    assertEquals("PURCHASE", getNestedField(actualPurchase10, actionTypeFieldPath).toString());
    assertEquals("i-1-181", getNestedField(actualPurchase10, actionSingleCartContentIdFieldPath));

    GenericRecord actualPurchase11 = contentIdToPurchases.get("s-1-11");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualPurchase11, requestIdFieldPath));
    assertEquals(
        "44444444-4444-4444-0000-000000000003",
        getNestedField(actualPurchase11, insertionIdFieldPath));
    assertEquals(
        "55555555-5555-5555-0000-000000000003",
        getNestedField(actualPurchase11, impressionIdFieldPath));
    assertEquals("s-1-11", getNestedField(actualPurchase11, insertionContentIdFieldPath));
    assertEquals("s-1-11", getNestedField(actualPurchase11, impressionContentIdFieldPath));
    assertEquals("PURCHASE", getNestedField(actualPurchase11, actionTypeFieldPath).toString());
    assertEquals("i-1-201", getNestedField(actualPurchase11, actionSingleCartContentIdFieldPath));

    GenericRecord actualPurchase12 = contentIdToPurchases.get("s-1-12");
    assertEquals(
        "33333333-3333-3333-0000-000000000001",
        getNestedField(actualPurchase12, requestIdFieldPath));
    assertEquals(
        "44444444-4444-4444-0000-000000000004",
        getNestedField(actualPurchase12, insertionIdFieldPath));
    assertEquals(
        "55555555-5555-5555-0000-000000000004",
        getNestedField(actualPurchase12, impressionIdFieldPath));
    assertEquals("s-1-12", getNestedField(actualPurchase12, insertionContentIdFieldPath));
    assertEquals("s-1-12", getNestedField(actualPurchase12, impressionContentIdFieldPath));
    assertEquals("PURCHASE", getNestedField(actualPurchase12, actionTypeFieldPath).toString());
    assertEquals("i-1-221", getNestedField(actualPurchase12, actionSingleCartContentIdFieldPath));
  }

  // TODO - add tests - https://toil.kitemaker.co/se6ONh-Promoted/hdRZPv-Promoted/items/813
  // TODO(PRO-1155) - add tests for right outer join.

  @DisplayName("One full event > Combine SDK and shadow traffic DeliveryLogs")
  @Test
  void combineSdkAndShadowTrafficDeliveryLogs() throws Exception {
    execute(
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1)
            .setMiniSdkRate(1.0f)
            .setShadowTrafficRate(1.0f)
            .setWriteLogUserId(true)
            .setWriteAnonUserId(true));

    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    assertSideOutputCount();
    // Gets delayed for the CombineDeliveryLog.
    long eventApiTimestamp = deliveryLog.get(0).getRequest().getTiming().getEventApiTimestamp();
    JoinedImpression.Builder joinedImpressionBuilder =
        createExpectedJoinedImpressionBuilder()
            .setHiddenApiRequest(
                HiddenApiRequest.newBuilder()
                    .setRequestId("33333333-3333-3333-0000-000000000002")
                    .setTiming(
                        Timing.newBuilder()
                            .setClientLogTimestamp(eventApiTimestamp)
                            .setEventApiTimestamp(eventApiTimestamp))
                    .setClientInfo(
                        ClientInfo.newBuilder()
                            .setClientType(ClientInfo.ClientType.PLATFORM_SERVER)
                            .setTrafficType(ClientInfo.TrafficType.SHADOW)))
            .setApiExecutionInsertion(
                toExpectedExecutionInsertionField(
                    deliveryLog.get(1).getExecution().getExecutionInsertion(0)))
            .setSdkExecution(
                DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.SDK).build());
    // SDK Request has a different TrafficType.
    // TODO - Might make sense to re-evaluate ClientType and TrafficType being in the paging_id
    joinedImpressionBuilder
        .getResponseBuilder()
        .getPagingInfoBuilder()
        .setPagingId("S7lJCYEfonAOnqh9LoN8Tg==");
    expectedEtlDirs.add("flat_actioned_response_insertion");
    assertJoinedOneFullImpressionAndAction(joinedImpressionBuilder);
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

    // TODO - This is broken.  Filtering currently happens on the denormalized outputs.
    expectedEtlDirs =
        ImmutableSet.of("tiny_action_path", "tiny_attributed_action", "tiny_joined_impression");
    assertEtlOutputCount();
    expectedDroppedDirs =
        ImmutableSet.of(
            "dropped_ignore_usage_attributed_action", "dropped_ignore_usage_joined_impression");
    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    assertSideOutputCount();
    assertEquals(null, new File(tempDir, "etl/joined_impression").list());
    assertEquals(null, new File(tempDir, "etl/attributed_action").list());
  }

  @DisplayName("Filter out non-buyer traffic")
  @Test
  void filterOutNonBuyer() throws Exception {
    // User property "is_staff" - Hipcamp.
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

    // TODO - This is broken.  Filtering currently happens on the denormalized outputs.
    expectedEtlDirs =
        ImmutableSet.of("tiny_action_path", "tiny_attributed_action", "tiny_joined_impression");
    assertEtlOutputCount();
    expectedDroppedDirs =
        ImmutableSet.of(
            "dropped_non_buyer_joined_impression", "dropped_non_buyer_attributed_action");
    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    assertSideOutputCount();
    assertEquals(null, new File(tempDir, "etl/joined_impression").list());
    assertEquals(null, new File(tempDir, "etl/attributed_action").list());
  }

  @DisplayName("Filter out bot traffic")
  @Test
  void filterOutBot() throws Exception {
    // User property "is_staff" - Hipcamp.
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
    assertEquals(null, new File(tempDir, "etl/attributed_action").list());
  }

  @DisplayName("One full event > Skip view join")
  @Test
  void skipViewJoin() throws Exception {
    execute(
        LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, detailLevel, 1)
            .setMiniSdkRate(0.0f)
            .setShadowTrafficRate(0.0f)
            .setMissingViewRate(1.0f)
            .setWriteLogUserId(true)
            .setWriteAnonUserId(true));

    expectedDebugDirs.add("debug_all_delivery_log_metadata");
    expectedDebugDirs.add("debug_all_delivery_log_request");
    expectedDebugDirs.add("debug_all_delivery_log_request_metadata");
    assertSideOutputCount();
    // CombinedDeliveryLog delays the Request by the window.
    JoinedImpression.Builder joinedImpressionBuilder = createExpectedJoinedImpressionBuilder();
    // sessionId currently comes from the View (which is not set).
    joinedImpressionBuilder.getIdsBuilder().clearSessionId();

    expectedEtlDirs.add("flat_actioned_response_insertion");
    assertJoinedOneFullImpressionAndAction(joinedImpressionBuilder);
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

  private void assertTinyJoinedImpression(TinyJoinedImpression tinyJoinedImpression)
      throws IOException {
    assertRecordInFile(
        "etl/tiny_joined_impression/dt=%s/hour=%s",
        tinyJoinedImpression, "etl/tiny_joined_impression");
  }

  private void assertTinyActionPath(TinyActionPath tinyActionPath) throws IOException {
    assertRecordInFile(
        "etl/tiny_action_path/dt=%s/hour=%s", tinyActionPath, "etl/tiny_action_path");
  }

  private void assertTinyAttributedActions(Iterable<TinyAttributedAction> tinyAttributedActions)
      throws IOException {
    assertRecordsInFile(
        "etl/tiny_attributed_action/dt=%s/hour=%s",
        tinyAttributedActions, "etl/tiny_attributed_action");
  }

  private void assertJoinedImpression(JoinedImpression.Builder builder) throws IOException {
    assertJoinedImpression(builder.build());
  }

  private void assertJoinedImpression(JoinedImpression joinedImpression) throws IOException {
    assertRecordInFile(
        "etl/joined_impression/dt=%s/hour=%s", joinedImpression, "etl/joined_impression");
  }

  private void assertAttributedAction(AttributedAction.Builder builder) throws IOException {
    assertAttributedAction(builder.build());
  }

  private void assertAttributedAction(AttributedAction attributedAction) throws IOException {
    assertRecordInFile(
        "etl/attributed_action/dt=%s/hour=%s", attributedAction, "etl/attributed_action");
  }

  private void assertAttributedActions(Iterable<AttributedAction> attributedActions)
      throws IOException {
    assertRecordsInFile(
        "etl/attributed_action/dt=%s/hour=%s", attributedActions, "etl/attributed_action");
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

  private void assertRecordsInFile(
      String template, Iterable<? extends GeneratedMessageV3> records, String message)
      throws IOException {
    String path = String.format(template, dt, hour);
    File[] files = assertPartFilesExist(new File(tempDir, path));
    assertAvroFiles(toFixedAvroGenericRecords(records), files, message);
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

  private JoinedImpression.Builder createExpectedJoinedImpressionBuilder() {
    long eventApiTimestamp = deliveryLog.get(0).getRequest().getTiming().getEventApiTimestamp();
    JoinedImpression.Builder joinedImpressionBuilder =
        JoinedImpression.newBuilder()
            .setIds(
                JoinedIdentifiers.newBuilder()
                    .setPlatformId(1L)
                    .setAnonUserId("00000000-0000-0000-0000-000000000001")
                    .setLogUserId("00000000-0000-0000-0000-000000000001")
                    .setViewId("22222222-2222-2222-0000-000000000001")
                    .setRequestId("33333333-3333-3333-0000-000000000001")
                    .setInsertionId("44444444-4444-4444-0000-000000000001")
                    .setImpressionId("55555555-5555-5555-0000-000000000001"))
            .setRequest(toExpectedRequestField(deliveryLog.get(0).getRequest()))
            .setRequestInsertion(
                toExpectedRequestInsertionField(deliveryLog.get(0).getRequest().getInsertion(0)))
            // Populated by the PopulatePagingId operator.
            .setResponse(
                toExpectedResponseField(
                    Response.newBuilder()
                        .setPagingInfo(
                            PagingInfo.newBuilder().setPagingId("xXPWoPIAYATGfTc/pNkDqw=="))
                        .build()))
            .setResponseInsertion(
                toExpectedResponseInsertionField(deliveryLog.get(0).getResponse().getInsertion(0)))
            .setApiExecution(
                DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.API).build())
            .setImpression(toExpectedImpressionField(impression.get(0)));
    if (deliveryLog.get(0).getExecution().getExecutionInsertionCount() > 0) {
      joinedImpressionBuilder.setApiExecutionInsertion(
          toExpectedExecutionInsertionField(
              deliveryLog.get(0).getExecution().getExecutionInsertion(0)));
    }
    // TODO - remove the Timing from the impression block.  It's redndant with JoinedEvent.
    joinedImpressionBuilder.setTiming(
        // TODO - wtf?  Not sure how this is possible.
        impression.get(0).getTiming().toBuilder()
            .setEventApiTimestamp(eventApiTimestamp)
            .setProcessingTimestamp(1L));
    UserInfoUtil.clearUserId(joinedImpressionBuilder);
    return joinedImpressionBuilder;
  }

  private TinyJoinedImpression toExpectedTinyJoinedImpression(
      JoinedImpression.Builder expectedJoinedImpressionBuilder) {
    return TinyJoinedImpression.newBuilder()
        .setInsertion(
            TinyInsertion.newBuilder()
                .setCommon(
                    TinyCommonInfo.newBuilder()
                        .setPlatformId(1L)
                        .setAnonUserId(expectedJoinedImpressionBuilder.getIds().getAnonUserId())
                        .setEventApiTimestamp(
                            expectedJoinedImpressionBuilder
                                .getRequest()
                                .getTiming()
                                .getEventApiTimestamp()))
                .setViewId(expectedJoinedImpressionBuilder.getIds().getViewId())
                .setRequestId(expectedJoinedImpressionBuilder.getIds().getRequestId())
                .setPagingId(
                    expectedJoinedImpressionBuilder.getResponse().getPagingInfo().getPagingId())
                .setCore(
                    TinyInsertionCore.newBuilder()
                        .setPosition(
                            expectedJoinedImpressionBuilder.getResponseInsertion().getPosition())
                        .setContentId(
                            expectedJoinedImpressionBuilder.getResponseInsertion().getContentId())
                        .setInsertionId(expectedJoinedImpressionBuilder.getIds().getInsertionId())))
        .setImpression(
            TinyImpression.newBuilder()
                .setCommon(
                    TinyCommonInfo.newBuilder()
                        .setPlatformId(1L)
                        .setAnonUserId(expectedJoinedImpressionBuilder.getIds().getAnonUserId())
                        .setEventApiTimestamp(
                            expectedJoinedImpressionBuilder.getTiming().getEventApiTimestamp()))
                .setInsertionId(expectedJoinedImpressionBuilder.getIds().getInsertionId())
                .setImpressionId(expectedJoinedImpressionBuilder.getIds().getImpressionId())
                .setContentId(expectedJoinedImpressionBuilder.getImpression().getContentId()))
        .build();
  }

  private void assertJoinedOneFullImpressionAndAction(
      JoinedImpression.Builder expectedJoinedImpressionBuilder) throws IOException {
    assertJoinedOneFullImpressionAndAction(expectedJoinedImpressionBuilder, false);
  }

  private void assertJoinedOneFullImpressionAndAction(
      JoinedImpression.Builder expectedJoinedImpressionBuilder, boolean debug) throws IOException {
    assertEtlOutputCount();

    // Assert tiny records.

    // TODO - assertTinyJoinedImpression

    TinyJoinedImpression expectedTinyJoinedImpression =
        toExpectedTinyJoinedImpression(expectedJoinedImpressionBuilder);
    assertTinyJoinedImpression(expectedTinyJoinedImpression);

    Action.Builder expectedActionBuilder = action.get(0).toBuilder();
    expectedActionBuilder.getTimingBuilder().setProcessingTimestamp(1L);
    AttributedAction.Builder attributedActionNearestBuilder =
        AttributedAction.newBuilder()
            .setAction(expectedActionBuilder.clone().setImpressionId(""))
            .setAttribution(
                Attribution.newBuilder()
                    .setModelId(AttributionModel.LATEST.id)
                    .setCreditMillis(1000))
            .setTouchpoint(
                Touchpoint.newBuilder()
                    .setJoinedImpression(expectedJoinedImpressionBuilder.clone()));
    AttributedAction.Builder attributedActionEvenBuilder = attributedActionNearestBuilder.clone();
    attributedActionEvenBuilder.getAttributionBuilder().setModelId(AttributionModel.EVEN.id);

    TinyAction expectedTinyAction =
        TinyAction.newBuilder()
            .setCommon(
                TinyCommonInfo.newBuilder()
                    .setPlatformId(1L)
                    .setAnonUserId(expectedActionBuilder.getUserInfo().getAnonUserId())
                    .setEventApiTimestamp(expectedActionBuilder.getTiming().getEventApiTimestamp()))
            .setActionId(expectedActionBuilder.getActionId())
            .setContentId(expectedActionBuilder.getContentId())
            .setImpressionId(expectedActionBuilder.getImpressionId())
            .setActionType(expectedActionBuilder.getActionType())
            .build();

    assertTinyActionPath(
        TinyActionPath.newBuilder()
            .setAction(expectedTinyAction)
            .addTouchpoints(
                TinyTouchpoint.newBuilder().setJoinedImpression(expectedTinyJoinedImpression))
            .build());

    assertTinyAttributedActions(
        ImmutableList.of(
            TinyAttributedAction.newBuilder()
                .setAction(expectedTinyAction)
                .setAttribution(attributedActionNearestBuilder.getAttribution())
                .setTouchpoint(
                    TinyTouchpoint.newBuilder().setJoinedImpression(expectedTinyJoinedImpression))
                .build(),
            TinyAttributedAction.newBuilder()
                .setAction(expectedTinyAction)
                .setAttribution(attributedActionEvenBuilder.getAttribution())
                .setTouchpoint(
                    TinyTouchpoint.newBuilder().setJoinedImpression(expectedTinyJoinedImpression))
                .build()));

    // Assert denormalized records.

    assertJoinedImpression(expectedJoinedImpressionBuilder.clone());

    assertAttributedActions(
        ImmutableList.of(
            attributedActionNearestBuilder.clone().build(),
            attributedActionEvenBuilder.clone().build()));

    FlatResponseInsertion.Builder flatResponseInsertion =
        FlatUtil.createFlatResponseInsertion(
            ImmutableList.of(expectedJoinedImpressionBuilder.clone().build()),
            ImmutableList.of(
                attributedActionNearestBuilder.clone().build(),
                attributedActionEvenBuilder.clone().build()));
    try {
      assertFlatResponseInsertion(flatResponseInsertion);
    } catch (Throwable e) {
      // Track down a flake.
      throw new RuntimeException(
          "Adding extra info.  flatResponseInsertion=" + flatResponseInsertion.clone().build(), e);
    }
  }

  // Separate function to reduce test lines of code.  The JoinedIdentifiers code clears a bunch of
  // these fields.
  private Request toExpectedRequestField(Request request) {
    Request.Builder builder =
        request.toBuilder()
            .clearPlatformId()
            .clearUserInfo()
            .clearRequestId()
            .clearViewId()
            .clearSessionId()
            .clearInsertion();
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
    deliveryLog = LogRequestFactory.pushDownToDeliveryLogs(logRequests);
    impression = LogRequestFactory.pushDownToImpressions(logRequests);
    action = LogRequestFactory.pushDownToActions(logRequests);

    // TODO - enable tests with Kafka.
    // https://github.com/apache/flink/blob/59905140b7d6a31f8c07fc8151f33f978e475bae/flink-connectors/flink-connector-kafka/src/test/java/org/apache/flink/streaming/connectors/kafka/table/UpsertKafkaTableITCase.java
  }

  private void executeJoinEvents() throws Exception {
    executeJoinEvents(job, deliveryLog, impression, action, contentDb);
  }

  // TODO - move to another file.

  // Helper method that sets operator uids to match the production inputs.
  private void executeJoinEvents(
      FlatOutputJob job,
      List<DeliveryLog> deliveryLogs,
      List<Impression> impressions,
      List<Action> actions,
      ContentDB contentDb)
      throws Exception {
    deliveryLogs = ImmutableList.copyOf(deliveryLogs);
    impressions = ImmutableList.copyOf(impressions);
    actions = ImmutableList.copyOf(actions);

    job.contentApiSegment.contentQueryClient = new MockClientContentFieldLookup(contentDb);
    job.contentApiSegment.enableInsertionContentLookup = true;
    job.contentApiSegment.enableActionContentLookup = true;
    job.executeJoinEvents(
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
  public static class MockClientContentFieldLookup implements ContentQueryClient {
    private static final long serialVersionUID = 1234567L;
    private final ContentDB contentDb;

    MockClientContentFieldLookup(ContentDB contentDb) {
      this.contentDb = contentDb;
    }

    @Override
    public Map<String, Map<String, Map<String, Object>>> apply(ContentQueryParams params) {
      // TODO - support projections.  Not important since the client code also filters.
      return params.contentIds().stream()
          .collect(
              Collector.of(
                  ImmutableMap.Builder<String, Map<String, Map<String, Object>>>::new,
                  (m, c) -> m.put(c, ImmutableMap.of("defaultSource", getSingleContentFields(c))),
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
