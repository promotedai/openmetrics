package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.JoinedImpression;

// TODO - restructure these utils.  Might make sense to merge.
/** Util for DeliveryExecution. */
public class DeliveryExecutionUtil {

  public static JoinedImpression clearAllDeliveryExecutionDetails(
      JoinedImpression joinedImpression) {
    return clearAllDeliveryExecutionDetails(joinedImpression.toBuilder()).build();
  }

  public static JoinedImpression.Builder clearAllDeliveryExecutionDetails(
      JoinedImpression.Builder builder) {
    if (builder.hasApiExecution()) {
      builder.clearApiExecution();
    }
    if (builder.hasApiExecutionInsertion()) {
      builder.clearApiExecutionInsertion();
    }
    if (builder.hasSdkExecution()) {
      builder.clearSdkExecution();
    }
    return builder;
  }

  public static AttributedAction clearAllDeliveryExecutionDetails(
      AttributedAction attributedAction) {
    return clearAllDeliveryExecutionDetails(attributedAction.toBuilder()).build();
  }

  public static AttributedAction.Builder clearAllDeliveryExecutionDetails(
      AttributedAction.Builder builder) {
    if (builder.getTouchpoint().hasJoinedImpression()) {
      clearAllDeliveryExecutionDetails(builder.getTouchpointBuilder().getJoinedImpressionBuilder());
    }
    return builder;
  }

  public static FlatResponseInsertion clearAllDeliveryExecutionDetails(FlatResponseInsertion flat) {
    return clearAllDeliveryExecutionDetails(flat.toBuilder()).build();
  }

  public static FlatResponseInsertion.Builder clearAllDeliveryExecutionDetails(
      FlatResponseInsertion.Builder builder) {
    if (builder.hasApiExecution()) {
      builder.clearApiExecution();
    }
    if (builder.hasApiExecutionInsertion()) {
      builder.clearApiExecutionInsertion();
    }
    if (builder.hasSdkExecution()) {
      builder.clearSdkExecution();
    }
    return builder;
  }
}
