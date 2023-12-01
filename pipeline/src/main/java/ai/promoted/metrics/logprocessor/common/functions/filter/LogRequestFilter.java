package ai.promoted.metrics.logprocessor.common.functions.filter;

import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapAction;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapAutoView;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapCohortMembership;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapDeliveryLog;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapDiagnostics;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapImpression;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapUser;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapView;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO - Switch to using FlatMaps directly.  This is a breaking change.  It might be safe to roll
// it out incrementally.
/**
 * Converts a stream of LogRequests to a denormalized stream of events as side output. The "primary"
 * output is actually the raw LogRequests as recevied.
 */
public final class LogRequestFilter extends ProcessFunction<LogRequest, LogRequest> {
  private static final Logger LOGGER = LogManager.getLogger(LogRequestFilter.class);

  private static final OutputTag<User> USER_TAG = new OutputTag<User>("user-events") {};
  private static final OutputTag<CohortMembership> COHORT_MEMBERSHIP_TAG =
      new OutputTag<CohortMembership>("cohort-membership-events") {};
  private static final OutputTag<AutoView> AUTO_VIEW_TAG =
      new OutputTag<AutoView>("auto-view-events") {};
  private static final OutputTag<View> VIEW_TAG = new OutputTag<View>("view-events") {};
  private static final OutputTag<DeliveryLog> DELIVERY_LOG_TAG =
      new OutputTag<DeliveryLog>("delivery-log-events") {};
  private static final OutputTag<Impression> IMPRESSION_TAG =
      new OutputTag<Impression>("impression-events") {};
  private static final OutputTag<Action> ACTION_TAG = new OutputTag<Action>("action-events") {};
  private static final OutputTag<Diagnostics> DIAGNOSTICS_TAG =
      new OutputTag<Diagnostics>("diagnostics-events") {};
  private final PushDownAndFlatMapUser userPushDown = new PushDownAndFlatMapUser();
  private final PushDownAndFlatMapCohortMembership cohortMembershipPushDown =
      new PushDownAndFlatMapCohortMembership();
  private final PushDownAndFlatMapAutoView autoViewPushDown = new PushDownAndFlatMapAutoView();
  private final PushDownAndFlatMapView viewPushDown = new PushDownAndFlatMapView();
  private final PushDownAndFlatMapDeliveryLog deliveryLogPushDown =
      new PushDownAndFlatMapDeliveryLog();
  private final PushDownAndFlatMapImpression impressionPushDown =
      new PushDownAndFlatMapImpression();
  private final PushDownAndFlatMapAction actionPushDown = new PushDownAndFlatMapAction();
  private final PushDownAndFlatMapDiagnostics diagnosticsPushDown =
      new PushDownAndFlatMapDiagnostics();

  public static SideOutputDataStream<User> getUserStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(USER_TAG);
  }

  public static SideOutputDataStream<CohortMembership> getCohortMembershipStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(COHORT_MEMBERSHIP_TAG);
  }

  public static SideOutputDataStream<AutoView> getAutoViewStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(AUTO_VIEW_TAG);
  }

  public static SideOutputDataStream<View> getViewStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(VIEW_TAG);
  }

  public static SideOutputDataStream<DeliveryLog> getDeliveryLogStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(DELIVERY_LOG_TAG);
  }

  public static SideOutputDataStream<Impression> getImpressionStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(IMPRESSION_TAG);
  }

  public static SideOutputDataStream<Action> getActionStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(ACTION_TAG);
  }

  public static SideOutputDataStream<Diagnostics> getDiagnosticsStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(DIAGNOSTICS_TAG);
  }

  @Override
  public void processElement(LogRequest in, Context ctx, Collector<LogRequest> out) {
    out.collect(in);
    // TODO: check platform id, uuid empty primary keys, lower case all ids
    long timestamp = ctx.timestamp();
    long watermark = ctx.timerService().currentWatermark();
    LOGGER.trace(
        "LogRequestFilter instance: {} ts: {} watermark: {} isLate: {}",
        System.identityHashCode(this),
        timestamp,
        watermark,
        timestamp < watermark);

    in.getUserList().forEach(i -> ctx.output(USER_TAG, userPushDown.pushDownFields(i, in)));
    in.getCohortMembershipList()
        .forEach(
            i -> ctx.output(COHORT_MEMBERSHIP_TAG, cohortMembershipPushDown.pushDownFields(i, in)));
    in.getAutoViewList()
        .forEach(i -> ctx.output(AUTO_VIEW_TAG, autoViewPushDown.pushDownFields(i, in)));
    in.getViewList().forEach(i -> ctx.output(VIEW_TAG, viewPushDown.pushDownFields(i, in)));
    in.getDeliveryLogList()
        .forEach(i -> ctx.output(DELIVERY_LOG_TAG, deliveryLogPushDown.pushDownFields(i, in)));
    in.getImpressionList()
        .forEach(i -> ctx.output(IMPRESSION_TAG, impressionPushDown.pushDownFields(i, in)));
    in.getActionList().forEach(i -> ctx.output(ACTION_TAG, actionPushDown.pushDownFields(i, in)));
    in.getDiagnosticsList()
        .forEach(i -> ctx.output(DIAGNOSTICS_TAG, diagnosticsPushDown.pushDownFields(i, in)));
  }
}
