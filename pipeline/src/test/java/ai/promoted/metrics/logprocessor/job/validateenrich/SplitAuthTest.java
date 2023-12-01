package ai.promoted.metrics.logprocessor.job.validateenrich;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import ai.promoted.proto.common.UserInfo;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SplitAuthTest {
  protected Collector<MockObject> mockOut;
  protected ProcessFunction<MockObject, MockObject>.Context mockContext;
  protected SplitAuth operator;

  @BeforeEach
  public void setUp() {
    mockOut = Mockito.mock(Collector.class);
    mockContext = Mockito.mock(ProcessFunction.Context.class);
    operator = new SplitAuth<>(MockObject::getUserInfo, MockObject.class);
  }

  @Test
  public void hasAuthUserId() throws Exception {
    MockObject mockObject = new MockObject();
    mockObject.userInfo = UserInfo.newBuilder().setUserId("userId1").build();
    operator.processElement(mockObject, mockContext, mockOut);
    verifyNoInteractions(mockContext);
    verify(mockOut).collect(mockObject);
  }

  @Test
  public void noAuthUserId() throws Exception {
    MockObject mockObject = new MockObject();
    operator.processElement(mockObject, mockContext, mockOut);
    verify(mockContext).output(operator.getFilteredOutTag(), mockObject);
    verifyNoInteractions(mockOut);
  }

  /** A mock for this test. */
  private static final class MockObject {
    UserInfo userInfo = UserInfo.getDefaultInstance();

    UserInfo getUserInfo() {
      return userInfo;
    }
  }
}
