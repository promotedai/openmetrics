package ai.promoted.metrics.logprocessor.common.fakedatagenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentType;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.FakeInsertionSurface;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.LogRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class LogRequestIteratorTest {

  // Arbitrary time when writing test.
  private static final long TIME_MS = 1639607262000L;

  // It's a lot of work to assert full properties.  Other tests do that .
  @Test
  public void counts() {
    List<LogRequest> logRequests =
        ImmutableList.copyOf(
            new LogRequestIterator(LogRequestFactory.createLogRequestOptionsBuilder(TIME_MS, 2)));
    RecordCounts counts = new RecordCounts();
    counts.addCounts(logRequests);

    assertEquals(2, counts.numUserRecords);
    assertEquals(8, counts.numViewRecords);
    assertEquals(16, counts.numDeliveryLogRecords);
    assertEquals(32, counts.numImpressionRecords);
    // LogRequestIterator has logic that produces at least one 1 navigate.  The random numbers
    // causes the
    // default case to not have any actions.
    assertEquals(2, counts.numActionRecords);
  }

  @Test
  public void counts_withRedundantImpressions() {
    LogRequestIterator iterator =
        new LogRequestIterator(
            LogRequestFactory.createLogRequestOptionsBuilder(TIME_MS, 2)
                .setShadowTrafficRate(0.0f)
                .setRedundantImpressionRate(0.5f)
                .setResponseInsertionsPerRequest(12)
                .setMaxRedundantImpressionsPerDeliveryLog(5)
                .setImpressionNavigateRate(0.1f)
                .setNavigateCheckoutRate(0.2f)
                .setCheckoutPurchaseRate(0.3f));
    List<LogRequest> logRequests = ImmutableList.copyOf(iterator);
    RecordCounts counts = new RecordCounts();
    counts.addCounts(logRequests);

    assertEquals(2, counts.numUserRecords);
    assertEquals(8, counts.numViewRecords);
    assertEquals(16, counts.numDeliveryLogRecords);
    assertEquals(624, counts.numImpressionRecords);
    assertEquals(19, counts.numActionRecords);
    assertEquals(
        ImmutableMap.of(
            ActionType.NAVIGATE, 16,
            ActionType.CHECKOUT, 3),
        counts.actionTypeToNumRecords);
  }

  // Used to avoid probability logic.
  @Test
  public void noProbability_largeCounts() {
    LogRequestIterator iterator =
        new LogRequestIterator(
            LogRequestFactory.createLogRequestOptionsBuilder(
                    TIME_MS, LogRequestFactory.DetailLevel.FULL, 5)
                .setShadowTrafficRate(0.0f)
                .setRedundantImpressionRate(0.0f)
                .setResponseInsertionsPerRequest(12)
                .setMaxRedundantImpressionsPerDeliveryLog(5)
                .setImpressionNavigateRate(1f)
                .setNavigateCheckoutRate(1f)
                .setCheckoutPurchaseRate(1f));
    List<LogRequest> logRequests = ImmutableList.copyOf(iterator);
    RecordCounts counts = new RecordCounts();
    counts.addCounts(logRequests);

    assertEquals(5, counts.numUserRecords);
    assertEquals(125, counts.numViewRecords);
    assertEquals(625, counts.numDeliveryLogRecords);
    assertEquals(7500, counts.numResponseInsertionLogRecords);
    assertEquals(7500, counts.numImpressionRecords);
    assertEquals(22500, counts.numActionRecords);
    assertEquals(
        ImmutableMap.of(
            ActionType.NAVIGATE, 7500,
            ActionType.CHECKOUT, 7500,
            ActionType.PURCHASE, 7500),
        counts.actionTypeToNumRecords);
  }

  // Goal is to make sure the probability works okay.  We need a larger number of events
  // for the rates to get closer to the expected rates.
  @Test
  public void probability_largeCounts() {
    LogRequestIterator iterator =
        new LogRequestIterator(
            LogRequestFactory.createLogRequestOptionsBuilder(
                    TIME_MS, LogRequestFactory.DetailLevel.FULL, 5)
                .setShadowTrafficRate(0.0f)
                .setRedundantImpressionRate(0.0f)
                .setResponseInsertionsPerRequest(12)
                .setMaxRedundantImpressionsPerDeliveryLog(5)
                .setImpressionNavigateRate(0.5f)
                .setNavigateCheckoutRate(0.5f)
                .setCheckoutPurchaseRate(0.5f));
    List<LogRequest> logRequests = ImmutableList.copyOf(iterator);
    RecordCounts counts = new RecordCounts();
    counts.addCounts(logRequests);

    assertEquals(5, counts.numUserRecords);
    assertEquals(125, counts.numViewRecords);
    assertEquals(625, counts.numDeliveryLogRecords);
    assertEquals(7500, counts.numResponseInsertionLogRecords);
    assertEquals(7500, counts.numImpressionRecords);
    assertEquals(6567, counts.numActionRecords);
    assertEquals(
        ImmutableMap.of(
            ActionType.NAVIGATE, 3750,
            ActionType.CHECKOUT, 1875,
            ActionType.PURCHASE, 942),
        counts.actionTypeToNumRecords);
  }

  @Test
  public void twoRequestLevels_storesToItems() {
    LogRequestIterator iterator =
        new LogRequestIterator(
            LogRequestFactory.createLogRequestOptionsBuilder(
                    TIME_MS, LogRequestFactory.DetailLevel.FULL, 2)
                .setShadowTrafficRate(0.0f)
                .setRedundantImpressionRate(0.5f)
                .setResponseInsertionsPerRequest(12)
                .setMaxRedundantImpressionsPerDeliveryLog(5)
                .setRequestInsertionSurface0(
                    FakeInsertionSurface.create(ContentType.STORE, true, true))
                .setRequestInsertionSurface1(
                    FakeInsertionSurface.create(ContentType.ITEM, true, true))
                .setImpressionNavigateRate(0.1f)
                .setNavigateCheckoutRate(0.25f)
                .setCheckoutPurchaseRate(0.50f));
    List<LogRequest> logRequests = ImmutableList.copyOf(iterator);
    RecordCounts counts = new RecordCounts();
    counts.addCounts(logRequests);

    assertEquals(2, counts.numUserRecords);
    assertEquals(24, counts.numViewRecords);
    assertEquals(48, counts.numDeliveryLogRecords);
    assertEquals(1680, counts.numImpressionRecords);
    assertEquals(70, counts.numActionRecords);
    // TODO - increase to get purchase rate.
    assertEquals(
        ImmutableMap.of(
            ActionType.NAVIGATE, 55,
            ActionType.CHECKOUT, 11,
            ActionType.PURCHASE, 4),
        counts.actionTypeToNumRecords);
  }

  @Test
  public void twoRequestLevels_cart() {
    LogRequestIterator iterator =
        new LogRequestIterator(
            LogRequestFactory.createLogRequestOptionsBuilder(
                    TIME_MS, LogRequestFactory.DetailLevel.FULL, 2)
                .setSetupForInferredIds(false)
                .setUsers(1)
                .setSessionsPerUser(1)
                // TODO - why does setViewsPerSession matter?
                .setViewsPerSession(2)
                .setAutoViewsPerSession(2)
                .setRequestsPerViews(2)
                .setResponseInsertionsPerRequest(4)
                .setMiniSdkRate(0.0f)
                .setShadowTrafficRate(0.0f)
                // TODO - does impression rate impact this?
                .setImpressionNavigateRate(1.0f)
                .setNavigateCheckoutRate(0.25f)
                .setNavigateAddToCartRate(0.5f)
                .setCheckoutPurchaseRate(1.0f)
                // TODO - weird things happen if we don't log impression and actions on these
                // surfaces.
                .setRequestInsertionSurface0(
                    FakeInsertionSurface.create(ContentType.STORE, false, false))
                .setRequestInsertionSurface1(
                    Optional.of(FakeInsertionSurface.create(ContentType.ITEM, true, true))));
    List<LogRequest> logRequests = ImmutableList.copyOf(iterator);
    RecordCounts counts = new RecordCounts();
    counts.addCounts(logRequests);

    assertEquals(1, counts.numUserRecords);
    assertEquals(18, counts.numViewRecords);
    assertEquals(36, counts.numDeliveryLogRecords);
    assertEquals(128, counts.numImpressionRecords);
    assertEquals(256, counts.numActionRecords);
    // TODO - increase to get purchase rate.
    assertEquals(
        ImmutableMap.of(
            ActionType.NAVIGATE, 128,
            ActionType.ADD_TO_CART, 64,
            ActionType.CHECKOUT, 32,
            ActionType.PURCHASE, 32),
        counts.actionTypeToNumRecords);
  }

  @Test
  public void matchesRateOption() {
    assertTrue(LogRequestIterator.matchesRateOption(0.5f, "a", "matchesRateOption"));
    assertTrue(LogRequestIterator.matchesRateOption(0.5f, "b", "matchesRateOption"));
    assertFalse(LogRequestIterator.matchesRateOption(0.5f, "c", "matchesRateOption"));
    assertTrue(LogRequestIterator.matchesRateOption(0.5f, "d", "matchesRateOption"));
    assertTrue(LogRequestIterator.matchesRateOption(0.5f, "e", "matchesRateOption"));
    assertTrue(LogRequestIterator.matchesRateOption(0.5f, "f", "matchesRateOption"));
  }

  @Test
  public void fromRateToCount() {
    assertEquals(5, LogRequestIterator.fromRateToCount(0.5f, 10, "a", "fromRateToCount"));
    assertEquals(1, LogRequestIterator.fromRateToCount(0.5f, 1, "a", "fromRateToCount"));
    assertEquals(0, LogRequestIterator.fromRateToCount(0.5f, 1, "a", "fromRateToCount3"));
    assertEquals(0, LogRequestIterator.fromRateToCount(0.5f, 1, "b", "fromRateToCount"));
    assertEquals(1, LogRequestIterator.fromRateToCount(0.5f, 1, "c", "fromRateToCount"));
    assertEquals(1, LogRequestIterator.fromRateToCount(0.5f, 1, "d", "fromRateToCount"));
    assertEquals(1, LogRequestIterator.fromRateToCount(0.5f, 1, "e", "fromRateToCount"));
    assertEquals(1, LogRequestIterator.fromRateToCount(0.5f, 1, "f", "fromRateToCount"));
  }

  // TODO - add a property that has entity types so we can count them.
  private static final class RecordCounts {
    int numUserRecords = 0;
    int numViewRecords = 0;
    int numDeliveryLogRecords = 0;
    int numResponseInsertionLogRecords = 0;
    int numImpressionRecords = 0;
    int numActionRecords = 0;
    Map<ActionType, Integer> actionTypeToNumRecords = Maps.newHashMap();

    void addCounts(Collection<LogRequest> logRequests) {
      logRequests.stream().forEach(this::addCounts);
    }

    void addCounts(LogRequest logRequest) {
      numUserRecords += logRequest.getUserCount();
      numViewRecords += logRequest.getViewCount();
      numDeliveryLogRecords += logRequest.getDeliveryLogCount();
      logRequest
          .getDeliveryLogList()
          .forEach(
              deliveryLog -> {
                numResponseInsertionLogRecords += deliveryLog.getResponse().getInsertionCount();
              });
      numImpressionRecords += logRequest.getImpressionCount();
      numActionRecords += logRequest.getActionCount();
      logRequest
          .getActionList()
          .forEach(
              action -> {
                actionTypeToNumRecords.put(
                    action.getActionType(),
                    actionTypeToNumRecords.getOrDefault(action.getActionType(), 0) + 1);
              });
    }
  }
}
