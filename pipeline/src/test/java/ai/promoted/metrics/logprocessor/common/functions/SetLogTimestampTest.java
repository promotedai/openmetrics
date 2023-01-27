package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.functions.SetLogTimestamp;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.event.Action;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


public class SetLogTimestampTest {

    private ProcessFunction.Context mockContext;
    private Collector mockCollector;

    @BeforeEach
    public void setUp() {
        mockContext = Mockito.mock(ProcessFunction.Context.class);
        mockCollector = Mockito.mock(Collector.class);
    }

    @Test
    public void processElement() throws Exception {
        Mockito.when(mockContext.timestamp())
            .thenReturn(1000L);
        Action action = Action.getDefaultInstance();
        new SetLogTimestamp<Action, Action.Builder>(Action::toBuilder, Action.Builder::getTimingBuilder, Action.Builder::build)
            .processElement(action, mockContext, mockCollector);
        Mockito.verify(mockCollector).collect(action.toBuilder()
            .setTiming(Timing.newBuilder().setLogTimestamp(1000L))
            .build());
    }
}
