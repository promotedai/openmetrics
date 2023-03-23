package ai.promoted.metrics.logprocessor.job.counter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.counter.FeatureId;
import ai.promoted.proto.delivery.internal.features.AggMetric;
import ai.promoted.proto.delivery.internal.features.CountType;
import ai.promoted.proto.delivery.internal.features.CountWindow;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class FeatureIdTest {
  @ParameterizedTest
  @CsvSource({
    "1056802, COUNT_IMPRESSION, 1, HOURS",
    "1056870, COUNT_PURCHASE, 7, DAYS",
    "1064938, SUM_PURCHASE_GMV, 90, DAYS"
  })
  void itemDeviceCount(long expected, AggMetric metric, int bucket, ChronoUnit windowUnit) {
    assertEquals(expected, FeatureId.itemDeviceCount(metric, bucket, windowUnit));
    assertEquals(CountType.ITEM_DEVICE_COUNT, FeatureId.countType(expected));
    assertEquals(metric, FeatureId.aggMetric(expected));
    assertEquals(FeatureId.getCountWindow(bucket, windowUnit), FeatureId.countWindow(expected));
  }

  @ParameterizedTest
  @CsvSource({
    "1237060, true, COUNT_NAVIGATE, 1, DAYS",
    "1237126, true, COUNT_ADD_TO_CART, 7, DAYS",
    "1147144, false, COUNT_CHECKOUT, 30, DAYS",
    "1147242, false, COUNT_MAKE_OFFER, 90, DAYS"
  })
  void userCount(
      long expected, boolean isLogUser, AggMetric metric, int bucket, ChronoUnit windowUnit) {
    assertEquals(expected, FeatureId.userCount(isLogUser, metric, bucket, windowUnit));
    assertEquals(
        (isLogUser ? CountType.LOG_USER_COUNT : CountType.USER_COUNT),
        FeatureId.countType(expected));
    assertEquals(metric, FeatureId.aggMetric(expected));
    assertEquals(FeatureId.getCountWindow(bucket, windowUnit), FeatureId.countWindow(expected));
  }

  @ParameterizedTest
  @CsvSource({
    "1351840, true, COUNT_SHARE",
    "1351968, true, COUNT_UNLIKE",
    "1335520, false, COUNT_COMMENT",
    "1335744, false, COUNT_COMPLETE_SIGN_IN"
  })
  void lastUserContentTimestamp(long expected, boolean isLogUser, AggMetric metric) {
    assertEquals(expected, FeatureId.lastUserContentTimestamp(isLogUser, metric));
    assertEquals(
        (isLogUser ? CountType.LOG_USER_ITEM_HOURS_AGO : CountType.USER_ITEM_HOURS_AGO),
        FeatureId.countType(expected));
    assertEquals(metric, FeatureId.aggMetric(expected));
    assertEquals(CountWindow.NONE, FeatureId.countWindow(expected));
  }

  @ParameterizedTest
  @CsvSource({
    "1343690, true, COUNT_LIKE",
    "1343914, true, COUNT_ANSWER_QUESTION",
    "1327434, false, COUNT_REMOVE_FROM_CART",
    "1327594, false, COUNT_COMPLETE_SIGN_UP"
  })
  void lastUserContentCount(long expected, boolean isLogUser, AggMetric metric) {
    assertEquals(expected, FeatureId.lastUserContentCount(isLogUser, metric));
    assertEquals(
        (isLogUser ? CountType.LOG_USER_ITEM_COUNT : CountType.USER_ITEM_COUNT),
        FeatureId.countType(expected));
    assertEquals(metric, FeatureId.aggMetric(expected));
    assertEquals(CountWindow.DAY_90, FeatureId.countWindow(expected));
  }

  @ParameterizedTest
  @CsvSource({
    "1564832, true, COUNT_SHARE",
    "1564960, true, COUNT_UNLIKE",
    "1548512, false, COUNT_COMMENT",
    "1548736, false, COUNT_COMPLETE_SIGN_IN"
  })
  void lastUserQueryTimestamp(long expected, boolean isLogUser, AggMetric metric) {
    assertEquals(expected, FeatureId.lastUserQueryTimestamp(isLogUser, metric));
    assertEquals(
        (isLogUser ? CountType.LOG_USER_QUERY_HOURS_AGO : CountType.USER_QUERY_HOURS_AGO),
        FeatureId.countType(expected));
    assertEquals(metric, FeatureId.aggMetric(expected));
    assertEquals(CountWindow.NONE, FeatureId.countWindow(expected));
  }

  @ParameterizedTest
  @CsvSource({
    "1556682, true, COUNT_LIKE",
    "1556906, true, COUNT_ANSWER_QUESTION",
    "1540426, false, COUNT_REMOVE_FROM_CART",
    "1540586, false, COUNT_COMPLETE_SIGN_UP"
  })
  void lastUserQueryCount(long expected, boolean isLogUser, AggMetric metric) {
    assertEquals(expected, FeatureId.lastUserQueryCount(isLogUser, metric));
    assertEquals(
        (isLogUser ? CountType.LOG_USER_QUERY_COUNT : CountType.USER_QUERY_COUNT),
        FeatureId.countType(expected));
    assertEquals(metric, FeatureId.aggMetric(expected));
    assertEquals(CountWindow.DAY_90, FeatureId.countWindow(expected));
  }

  @Test
  void expandFeatures_crossProduct() {
    assertEquals(
        ImmutableSet.of(
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_IMPRESSION, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_IMPRESSION, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_NAVIGATE, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_NAVIGATE, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_PURCHASE, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_PURCHASE, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_ADD_TO_CART, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_ADD_TO_CART, CountWindow.DAY),
            FeatureId.featureId(CountType.LOG_USER_COUNT, AggMetric.COUNT_SHARE, CountWindow.HOUR),
            FeatureId.featureId(CountType.LOG_USER_COUNT, AggMetric.COUNT_SHARE, CountWindow.DAY),
            FeatureId.featureId(CountType.LOG_USER_COUNT, AggMetric.COUNT_LIKE, CountWindow.HOUR),
            FeatureId.featureId(CountType.LOG_USER_COUNT, AggMetric.COUNT_LIKE, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_COMMENT, CountWindow.HOUR),
            FeatureId.featureId(CountType.LOG_USER_COUNT, AggMetric.COUNT_COMMENT, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_CHECKOUT, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_CHECKOUT, CountWindow.DAY),
            FeatureId.featureId(CountType.LOG_USER_COUNT, AggMetric.COUNT_UNLIKE, CountWindow.HOUR),
            FeatureId.featureId(CountType.LOG_USER_COUNT, AggMetric.COUNT_UNLIKE, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_REMOVE_FROM_CART, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_REMOVE_FROM_CART, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_MAKE_OFFER, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_MAKE_OFFER, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_ASK_QUESTION, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_ASK_QUESTION, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_ANSWER_QUESTION, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_ANSWER_QUESTION, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_COMPLETE_SIGN_IN, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_COMPLETE_SIGN_IN, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_COMPLETE_SIGN_UP, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_COMPLETE_SIGN_UP, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_BOOKMARK, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_BOOKMARK, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_REMOVE_BOOKMARK, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_REMOVE_BOOKMARK, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_ADD_TO_LIST, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_ADD_TO_LIST, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_REMOVE_FROM_LIST, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.COUNT_REMOVE_FROM_LIST, CountWindow.DAY),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.SUM_PURCHASE_GMV, CountWindow.HOUR),
            FeatureId.featureId(
                CountType.LOG_USER_COUNT, AggMetric.SUM_PURCHASE_GMV, CountWindow.DAY)),
        FeatureId.expandFeatureIds(
            EnumSet.of(CountType.LOG_USER_COUNT),
            EnumSet.complementOf(EnumSet.of(AggMetric.UNKNOWN_AGGREGATE, AggMetric.UNRECOGNIZED)),
            EnumSet.of(CountWindow.HOUR, CountWindow.DAY)));
  }

  @Test
  void expandFeatures_metrics() {
    assertEquals(
        ImmutableSet.of(
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_IMPRESSION, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_IMPRESSION, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_NAVIGATE, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_NAVIGATE, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_PURCHASE, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_PURCHASE, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_ADD_TO_CART, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_ADD_TO_CART, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_SHARE, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_SHARE, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_LIKE, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_LIKE, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_COMMENT, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_COMMENT, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_CHECKOUT, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_CHECKOUT, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_UNLIKE, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_UNLIKE, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_REMOVE_FROM_CART, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_REMOVE_FROM_CART, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_MAKE_OFFER, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_MAKE_OFFER, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_ASK_QUESTION, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_ASK_QUESTION, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_ANSWER_QUESTION, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_ANSWER_QUESTION, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_COMPLETE_SIGN_IN, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_COMPLETE_SIGN_IN, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_COMPLETE_SIGN_UP, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_COMPLETE_SIGN_UP, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_BOOKMARK, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_BOOKMARK, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_REMOVE_BOOKMARK, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_REMOVE_BOOKMARK, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_ADD_TO_LIST, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_ADD_TO_LIST, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.COUNT_REMOVE_FROM_LIST, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.COUNT_REMOVE_FROM_LIST, CountWindow.NONE),
            FeatureId.featureId(
                CountType.USER_ITEM_COUNT, AggMetric.SUM_PURCHASE_GMV, CountWindow.DAY_90),
            FeatureId.featureId(
                CountType.USER_ITEM_HOURS_AGO, AggMetric.SUM_PURCHASE_GMV, CountWindow.NONE)),
        FeatureId.expandFeatureIds(
            ImmutableList.of(
                FeatureId.featureId(CountType.USER_ITEM_COUNT, null, CountWindow.DAY_90),
                FeatureId.featureId(CountType.USER_ITEM_HOURS_AGO, null, CountWindow.NONE)),
            EnumSet.complementOf(EnumSet.of(AggMetric.UNKNOWN_AGGREGATE, AggMetric.UNRECOGNIZED))));
  }
}
