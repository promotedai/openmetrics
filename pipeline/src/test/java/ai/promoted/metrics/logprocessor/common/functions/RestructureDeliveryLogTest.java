package ai.promoted.metrics.logprocessor.common.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.common.Properties;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class RestructureDeliveryLogTest {
  @Test
  public void reservedHeaders() {
    var expectedRequest =
        Request.newBuilder()
            .addInsertion(
                Insertion.newBuilder()
                    .setContentId("a")
                    .setRetrievalRank(1)
                    .setRetrievalScore(2.0f));
    var actualRequest =
        Request.newBuilder()
            .addAllInsertionMatrixHeaders(
                Arrays.asList("contentId", "retrievalRank", "retrievalScore"))
            .setInsertionMatrix(
                ListValue.newBuilder()
                    .addValues(
                        Value.newBuilder()
                            .setListValue(
                                ListValue.newBuilder()
                                    .addAllValues(
                                        Arrays.asList(
                                            Value.newBuilder().setStringValue("a").build(),
                                            Value.newBuilder().setNumberValue(1).build(),
                                            Value.newBuilder().setNumberValue(2.0f).build())))));
    assertEquals(
        DeliveryLog.newBuilder().setRequest(expectedRequest).build(),
        new RestructureDeliveryLog()
            .map(DeliveryLog.newBuilder().setRequest(actualRequest).build()));
  }

  @Test
  public void conflictingHeaders() {
    var expectedRequest =
        Request.newBuilder()
            .addInsertion(
                Insertion.newBuilder()
                    .setProperties(
                        Properties.newBuilder()
                            .setStruct(
                                Struct.newBuilder()
                                    .putFields(
                                        "1",
                                        Value.newBuilder()
                                            .setStructValue(
                                                Struct.newBuilder()
                                                    .putFields(
                                                        "3",
                                                        Value.newBuilder()
                                                            .setStringValue("c")
                                                            .build()))
                                            .build()))));
    var actualRequest =
        Request.newBuilder()
            .addAllInsertionMatrixHeaders(Arrays.asList("1", "1", "1.3"))
            .setInsertionMatrix(
                ListValue.newBuilder()
                    .addValues(
                        Value.newBuilder()
                            .setListValue(
                                ListValue.newBuilder()
                                    .addAllValues(
                                        Arrays.asList(
                                            Value.newBuilder().setStringValue("a").build(),
                                            Value.newBuilder().setStringValue("b").build(),
                                            Value.newBuilder().setStringValue("c").build())))));
    assertEquals(
        DeliveryLog.newBuilder().setRequest(expectedRequest).build(),
        new RestructureDeliveryLog()
            .map(DeliveryLog.newBuilder().setRequest(actualRequest).build()));
  }

  @Test
  public void combinedSubStructs() {
    var expectedRequest =
        Request.newBuilder()
            .addInsertion(
                Insertion.newBuilder()
                    .setProperties(
                        Properties.newBuilder()
                            .setStruct(
                                Struct.newBuilder()
                                    .putFields("1", Value.newBuilder().setStringValue("a").build())
                                    .putFields(
                                        "2",
                                        Value.newBuilder()
                                            .setStructValue(
                                                Struct.newBuilder()
                                                    .putFields(
                                                        "3",
                                                        Value.newBuilder()
                                                            .setStringValue("b")
                                                            .build())
                                                    .putFields(
                                                        "4",
                                                        Value.newBuilder()
                                                            .setNumberValue(4.0f)
                                                            .build()))
                                            .build()))));
    var actualRequest =
        Request.newBuilder()
            .addAllInsertionMatrixHeaders(Arrays.asList("1", "2.3", "2.4"))
            .setInsertionMatrix(
                ListValue.newBuilder()
                    .addValues(
                        Value.newBuilder()
                            .setListValue(
                                ListValue.newBuilder()
                                    .addAllValues(
                                        Arrays.asList(
                                            Value.newBuilder().setStringValue("a").build(),
                                            Value.newBuilder().setStringValue("b").build(),
                                            Value.newBuilder().setNumberValue(4.0f).build())))));
    assertEquals(
        DeliveryLog.newBuilder().setRequest(expectedRequest).build(),
        new RestructureDeliveryLog()
            .map(DeliveryLog.newBuilder().setRequest(actualRequest).build()));
  }

  @Test
  public void multipleInsertions() {
    var expectedRequest =
        Request.newBuilder()
            .addInsertion(
                Insertion.newBuilder()
                    .setProperties(
                        Properties.newBuilder()
                            .setStruct(
                                Struct.newBuilder()
                                    .putFields(
                                        "1", Value.newBuilder().setStringValue("a").build()))))
            .addInsertion(
                Insertion.newBuilder()
                    .setProperties(
                        Properties.newBuilder()
                            .setStruct(
                                Struct.newBuilder()
                                    .putFields(
                                        "1", Value.newBuilder().setStringValue("b").build()))));
    var actualRequest =
        Request.newBuilder()
            .addAllInsertionMatrixHeaders(Arrays.asList("1"))
            .setInsertionMatrix(
                ListValue.newBuilder()
                    .addAllValues(
                        Arrays.asList(
                            Value.newBuilder()
                                .setListValue(
                                    ListValue.newBuilder()
                                        .addAllValues(
                                            Arrays.asList(
                                                Value.newBuilder().setStringValue("a").build())))
                                .build(),
                            Value.newBuilder()
                                .setListValue(
                                    ListValue.newBuilder()
                                        .addAllValues(
                                            Arrays.asList(
                                                Value.newBuilder().setStringValue("b").build())))
                                .build())));
    assertEquals(
        DeliveryLog.newBuilder().setRequest(expectedRequest).build(),
        new RestructureDeliveryLog()
            .map(DeliveryLog.newBuilder().setRequest(actualRequest).build()));
  }
}
