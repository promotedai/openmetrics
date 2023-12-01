package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.Timing;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

public class BaseValidateTest<T> {

  protected static long PLATFORM_ID = 1L;
  protected static String ANON_USER_ID = "anonUser1";
  protected static String VIEW_ID = "view1";
  protected static String REQUEST_ID = "request1";
  protected static String INSERTION_ID = "insertion1";
  protected static String IMPRESSION_ID = "impression1";
  protected static String CONTENT_ID = "content1";
  protected static String ACTION_ID = "action1";
  protected static long CLIENT_LOG_TIMESTAMP = 1L;
  protected static long EVENT_API_TIMESTAMP = 2L;
  protected static long LOG_TIMESTAMP = 3L;
  protected Collector<T> mockOut;
  protected ProcessFunction<T, T>.Context mockContext;

  protected static ai.promoted.proto.common.Timing getProtoTiming() {
    return ai.promoted.proto.common.Timing.newBuilder()
        .setClientLogTimestamp(CLIENT_LOG_TIMESTAMP)
        .setEventApiTimestamp(EVENT_API_TIMESTAMP)
        .setLogTimestamp(LOG_TIMESTAMP)
        .build();
  }

  protected static Timing getAvroTiming() {
    return Timing.newBuilder()
        .setClientLogTimestamp(CLIENT_LOG_TIMESTAMP)
        .setEventApiTimestamp(EVENT_API_TIMESTAMP)
        .setLogTimestamp(LOG_TIMESTAMP)
        .build();
  }

  @BeforeEach
  public void setUp() {
    mockOut = Mockito.mock(Collector.class);
    mockContext = Mockito.mock(ProcessFunction.Context.class);
  }
}
