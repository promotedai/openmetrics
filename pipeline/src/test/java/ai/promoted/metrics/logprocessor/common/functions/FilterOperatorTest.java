package ai.promoted.metrics.logprocessor.common.functions;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class FilterOperatorTest {

  private static final OutputTag<String> FILTERED_OUT_TAG =
      new OutputTag<String>("filtered-out") {};

  private static boolean equalsAlpha(String value) {
    return "alpha".equals(value);
  }

  protected Collector<String> mockOut;
  protected ProcessFunction<String, String>.Context mockContext;
  protected FilterOperator<String> operator;

  @BeforeEach
  public void setUp() {
    mockOut = Mockito.mock(Collector.class);
    mockContext = Mockito.mock(ProcessFunction.Context.class);
    operator = new FilterOperator<String>(FilterOperatorTest::equalsAlpha, FILTERED_OUT_TAG);
  }

  @Test
  public void filter() throws Exception {
    operator.processElement("alpha", mockContext, mockOut);
    verify(mockContext, never()).output(FILTERED_OUT_TAG, "alpha");
    verify(mockOut).collect("alpha");
  }

  @Test
  public void filterOut() throws Exception {
    operator.processElement("beta", mockContext, mockOut);
    verify(mockContext).output(FILTERED_OUT_TAG, "beta");
    verify(mockOut, never()).collect("beta");
  }
}
