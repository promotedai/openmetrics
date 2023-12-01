package ai.promoted.metrics.logprocessor.common.functions;

import static ai.promoted.metrics.logprocessor.common.functions.DeliveryExecutionUtil.clearAllDeliveryExecutionDetails;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.delivery.DeliveryExecution;
import ai.promoted.proto.delivery.ExecutionServer;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.JoinedImpression;
import org.junit.jupiter.api.Test;

public class DeliveryExecutionUtilTest {
  // JoinedEvent.
  @Test
  public void clearAllDeliveryExecutionDetails_JoinedImpression_ApiExecution() {
    assertEquals(
        JoinedImpression.newBuilder().build(),
        clearAllDeliveryExecutionDetails(
            JoinedImpression.newBuilder()
                .setApiExecution(
                    DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.API))
                .build()));
  }

  @Test
  public void clearAllDeliveryExecutionDetails_JoinedImpression_ApiExecutionInsertion() {
    assertEquals(
        JoinedImpression.newBuilder().build(),
        clearAllDeliveryExecutionDetails(
            JoinedImpression.newBuilder()
                .setApiExecutionInsertion(Insertion.newBuilder().setContentId("a"))
                .build()));
  }

  @Test
  public void clearAllDeliveryExecutionDetails_JoinedEvent_SdkExecution() {
    assertEquals(
        JoinedImpression.newBuilder().build(),
        clearAllDeliveryExecutionDetails(
            JoinedImpression.newBuilder()
                .setSdkExecution(
                    DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.SDK))
                .build()));
  }

  // FlatResponseInsertion.
  @Test
  public void clearAllDeliveryExecutionDetails_FlatResponseInsertion_ApiExecution() {
    assertEquals(
        FlatResponseInsertion.newBuilder().build(),
        clearAllDeliveryExecutionDetails(
            FlatResponseInsertion.newBuilder()
                .setApiExecution(
                    DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.API))
                .build()));
  }

  @Test
  public void clearAllDeliveryExecutionDetails_FlatResponseInsertion_ApiExecutionInsertion() {
    assertEquals(
        FlatResponseInsertion.newBuilder().build(),
        clearAllDeliveryExecutionDetails(
            FlatResponseInsertion.newBuilder()
                .setApiExecutionInsertion(Insertion.newBuilder().setContentId("a"))
                .build()));
  }

  @Test
  public void clearAllDeliveryExecutionDetails_FlatResponseInsertion_SdkExecution() {
    assertEquals(
        FlatResponseInsertion.newBuilder().build(),
        clearAllDeliveryExecutionDetails(
            FlatResponseInsertion.newBuilder()
                .setSdkExecution(
                    DeliveryExecution.newBuilder().setExecutionServer(ExecutionServer.SDK))
                .build()));
  }
}
