package ai.promoted.metrics.logprocessor.common.functions;

import java.time.Clock;
import java.time.Duration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LogSlowOnTimerTest {

  private Clock mockClock;
  private KeyedCoProcessFunction<String, String, String, String> mockDelegate;
  private LogSlowOnTimer<String, String, String, String> func;

  @BeforeEach
  public void setUp() {
    mockClock = Mockito.mock(Clock.class);
    mockDelegate = Mockito.mock(KeyedCoProcessFunction.class);
    func = new LogSlowOnTimer<>(mockDelegate);
  }

  @Test
  public void fastEnough() throws Exception {
    Mockito.when(mockClock.millis())
        .thenReturn(1000L)
        .thenReturn(Duration.ofMinutes(10).toMillis());
    func.onTimer(1000L, null, null);
    Mockito.verify(mockDelegate).onTimer(1000L, null, null);
  }

  // This will make a LOGGER call.
  @Test
  public void tooSlow() throws Exception {
    Mockito.when(mockClock.millis())
        .thenReturn(1000L)
        .thenReturn(Duration.ofMinutes(11).toMillis());
    func.onTimer(1000L, null, null);
    Mockito.verify(mockDelegate).onTimer(1000L, null, null);
  }
}
