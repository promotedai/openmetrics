package ai.promoted.metrics.logprocessor.job.validateenrich;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.EnrichmentUnion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SplitUnionTest {
  protected Collector<DeliveryLog> mockOut;
  protected ProcessFunction<EnrichmentUnion, DeliveryLog>.Context mockContext;
  protected SplitUnion operator;

  @BeforeEach
  public void setUp() {
    mockOut = Mockito.mock(Collector.class);
    mockContext = Mockito.mock(ProcessFunction.Context.class);
    operator = new SplitUnion();
  }

  @Test
  public void splitCohortMembership() throws Exception {
    CohortMembership cohortMembership =
        CohortMembership.newBuilder().setMembershipId("mem1").build();
    operator.processElement(
        EnrichmentUnionUtil.toCohortMembershipUnion(cohortMembership), mockContext, mockOut);
    verify(mockContext).output(SplitUnion.COHORT_MEMBERSHIP_TAG, cohortMembership);
    verifyNoMoreInteractions(mockContext);
    verifyNoInteractions(mockOut);
  }

  @Test
  public void splitView() throws Exception {
    View view = View.newBuilder().setViewId("view1").build();
    operator.processElement(EnrichmentUnionUtil.toViewUnion(view), mockContext, mockOut);
    verify(mockContext).output(SplitUnion.VIEW_TAG, view);
    verifyNoMoreInteractions(mockContext);
    verifyNoInteractions(mockOut);
  }

  @Test
  public void splitDeliveryLog() throws Exception {
    DeliveryLog deliveryLog =
        DeliveryLog.newBuilder().setRequest(Request.newBuilder().setRequestId("req1")).build();
    operator.processElement(
        EnrichmentUnionUtil.toDeliveryLogUnion(deliveryLog), mockContext, mockOut);
    verify(mockOut).collect(deliveryLog);
    verifyNoInteractions(mockContext);
  }

  @Test
  public void splitImpression() throws Exception {
    Impression impression = Impression.newBuilder().setImpressionId("imp1").build();
    operator.processElement(
        EnrichmentUnionUtil.toImpressionUnion(impression), mockContext, mockOut);
    verify(mockContext).output(SplitUnion.IMPRESSION_TAG, impression);
    verifyNoMoreInteractions(mockContext);
    verifyNoInteractions(mockOut);
  }

  @Test
  public void splitAction() throws Exception {
    Action action = Action.newBuilder().setActionId("act1").build();
    operator.processElement(EnrichmentUnionUtil.toActionUnion(action), mockContext, mockOut);
    verify(mockContext).output(SplitUnion.ACTION_TAG, action);
    verifyNoMoreInteractions(mockContext);
    verifyNoInteractions(mockOut);
  }

  @Test
  public void splitDiagnostics() throws Exception {
    Diagnostics diagnostics = Diagnostics.newBuilder().setPlatformId(1L).build();
    operator.processElement(
        EnrichmentUnionUtil.toDiagnosticsUnion(diagnostics), mockContext, mockOut);
    verify(mockContext).output(SplitUnion.DIAGNOSTICS_TAG, diagnostics);
    verifyNoMoreInteractions(mockContext);
    verifyNoInteractions(mockOut);
  }
}
