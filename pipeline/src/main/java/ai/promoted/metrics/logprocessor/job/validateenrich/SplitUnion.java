package ai.promoted.metrics.logprocessor.job.validateenrich;

import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.EnrichmentUnion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Splits an EnrichmentUnion into different streams. DeliveryLog was picked as the main output since
 * it's the largest record.
 */
public class SplitUnion extends ProcessFunction<EnrichmentUnion, DeliveryLog> {
  public static final OutputTag<CohortMembership> COHORT_MEMBERSHIP_TAG =
      new OutputTag<>("cohort-membership") {};
  public static final OutputTag<View> VIEW_TAG = new OutputTag<>("view") {};
  public static final OutputTag<Impression> IMPRESSION_TAG = new OutputTag<>("impression") {};
  public static final OutputTag<Action> ACTION_TAG = new OutputTag<>("action") {};
  public static final OutputTag<Diagnostics> DIAGNOSTICS_TAG = new OutputTag<>("diagnostics") {};

  @Override
  public void processElement(
      EnrichmentUnion union,
      ProcessFunction<EnrichmentUnion, DeliveryLog>.Context ctx,
      Collector<DeliveryLog> out)
      throws Exception {
    if (union.hasCohortMembership()) {
      ctx.output(COHORT_MEMBERSHIP_TAG, union.getCohortMembership());
    } else if (union.hasView()) {
      ctx.output(VIEW_TAG, union.getView());
    } else if (union.hasDeliveryLog()) {
      out.collect(union.getDeliveryLog());
    } else if (union.hasImpression()) {
      ctx.output(IMPRESSION_TAG, union.getImpression());
    } else if (union.hasAction()) {
      ctx.output(ACTION_TAG, union.getAction());
    } else if (union.hasDiagnostics()) {
      ctx.output(DIAGNOSTICS_TAG, union.getDiagnostics());
    } else {
      throw new UnsupportedOperationException("Unsupported EnrichmentUnion=" + union);
    }
  }
}
