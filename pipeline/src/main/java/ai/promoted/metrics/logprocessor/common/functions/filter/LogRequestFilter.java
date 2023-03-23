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
import org.apache.flink.streaming.api.datastream.DataStream;
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

  @Override
  public void processElement(LogRequest in, Context ctx, Collector<LogRequest> out) {
    out.collect(in);

    String lowerCaseBatchLogUserId = in.getUserInfo().getLogUserId().toLowerCase();

    // TODO: check platform id, uuid empty primary keys, lower case all ids

    long timestamp = ctx.timestamp();
    long watermark = ctx.timerService().currentWatermark();
    LOGGER.trace(
        "LogRequestFilter instance: {} ts: {} watermark: {} isLate: {}",
        System.identityHashCode(this),
        timestamp,
        watermark,
        timestamp < watermark);
    in.getUserList()
        .forEach(
            i ->
                ctx.output(
                    USER_TAG,
                    PushDownAndFlatMapUser.pushDownFields(i, in, lowerCaseBatchLogUserId)));
    in.getCohortMembershipList()
        .forEach(
            i ->
                ctx.output(
                    COHORT_MEMBERSHIP_TAG,
                    PushDownAndFlatMapCohortMembership.pushDownFields(
                        i, in, lowerCaseBatchLogUserId)));
    in.getAutoViewList()
        .forEach(
            i ->
                ctx.output(
                    AUTO_VIEW_TAG,
                    PushDownAndFlatMapAutoView.pushDownFields(i, in, lowerCaseBatchLogUserId)));
    in.getViewList()
        .forEach(
            i ->
                ctx.output(
                    VIEW_TAG,
                    PushDownAndFlatMapView.pushDownFields(i, in, lowerCaseBatchLogUserId)));
    in.getDeliveryLogList()
        .forEach(
            i ->
                ctx.output(
                    DELIVERY_LOG_TAG,
                    PushDownAndFlatMapDeliveryLog.pushDownFields(i, in, lowerCaseBatchLogUserId)));
    in.getImpressionList()
        .forEach(
            i ->
                ctx.output(
                    IMPRESSION_TAG,
                    PushDownAndFlatMapImpression.pushDownFields(i, in, lowerCaseBatchLogUserId)));
    in.getActionList()
        .forEach(
            i ->
                ctx.output(
                    ACTION_TAG,
                    PushDownAndFlatMapAction.pushDownFields(i, in, lowerCaseBatchLogUserId)));
    in.getDiagnosticsList()
        .forEach(
            i ->
                ctx.output(
                    DIAGNOSTICS_TAG,
                    PushDownAndFlatMapDiagnostics.pushDownFields(i, in, lowerCaseBatchLogUserId)));
  }

  public static DataStream<User> getUserStream(SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(USER_TAG);
  }

  public static DataStream<CohortMembership> getCohortMembershipStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(COHORT_MEMBERSHIP_TAG);
  }

  public static DataStream<AutoView> getAutoViewStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(AUTO_VIEW_TAG);
  }

  public static DataStream<View> getViewStream(SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(VIEW_TAG);
  }

  public static DataStream<DeliveryLog> getDeliveryLogStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(DELIVERY_LOG_TAG);
  }

  public static DataStream<Impression> getImpressionStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(IMPRESSION_TAG);
  }

  public static DataStream<Action> getActionStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(ACTION_TAG);
  }

  public static DataStream<Diagnostics> getDiagnosticsStream(
      SingleOutputStreamOperator<LogRequest> filtered) {
    return filtered.getSideOutput(DIAGNOSTICS_TAG);
  }
}
