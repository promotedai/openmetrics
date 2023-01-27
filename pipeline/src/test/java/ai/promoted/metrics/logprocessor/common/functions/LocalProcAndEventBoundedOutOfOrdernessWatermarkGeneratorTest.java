package ai.promoted.metrics.logprocessor.common.functions;

import com.google.common.collect.FluentIterable;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Duration;


public class LocalProcAndEventBoundedOutOfOrdernessWatermarkGeneratorTest {
    private static final Duration MAX_OUT_OF_ORDER = Duration.ofMillis(10);
    private static final Duration IDLE_DETECTION_THRESHOLD = Duration.ofMillis(1000);

    private Clock mockClock;
    private LocalProcAndEventBoundedOutOfOrdernessWatermarkGenerator<Long> generator;
    private WatermarkOutput mockOutput;
    private ArgumentCaptor<Watermark> capturedWatermarks;

    @BeforeEach
    public void setUp() {
        mockClock = Mockito.mock(Clock.class);
        generator = new LocalProcAndEventBoundedOutOfOrdernessWatermarkGenerator<>(
                mockClock, MAX_OUT_OF_ORDER, IDLE_DETECTION_THRESHOLD, IDLE_DETECTION_THRESHOLD);
        mockOutput = Mockito.mock(WatermarkOutput.class);
        capturedWatermarks = ArgumentCaptor.forClass(Watermark.class);
    }

    @Test
    public void noEvents() throws Exception {
        Mockito.when(mockClock.millis())
            .thenReturn(1700L)
            .thenReturn(2500L)
            .thenReturn(2501L)
            .thenReturn(3500L)
            .thenReturn(3502L)
            .thenReturn(3900L)
            .thenReturn(4000L)
            .thenReturn(4300L)
            .thenReturn(1000000L);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);

        Mockito.verify(mockClock, Mockito.times(10)).millis();
        Mockito.verify(mockOutput, Mockito.times(10)).emitWatermark(capturedWatermarks.capture());
        Mockito.verifyNoMoreInteractions(mockOutput);

        Assertions.assertIterableEquals(
                FluentIterable.of(
                    new Watermark(Long.MIN_VALUE),
                    new Watermark(Long.MIN_VALUE),
                    new Watermark(Long.MIN_VALUE),
                    new Watermark(Long.MIN_VALUE),
                    new Watermark(2501),
                    new Watermark(2899),
                    new Watermark(2999),
                    new Watermark(3299),
                    new Watermark(998999),
                    new Watermark(998999)),
                capturedWatermarks.getAllValues());
    }

    @Test
    public void singleBurst_increasing() throws Exception {
        // Event burst.
        Mockito.when(mockClock.millis()).thenReturn(10L).thenReturn(15L);
        generator.onEvent(100L, 100L, mockOutput);
        generator.onPeriodicEmit(mockOutput);
        Mockito.when(mockClock.millis()).thenReturn(500L).thenReturn(550L);
        generator.onEvent(500L, 500L, mockOutput);
        generator.onPeriodicEmit(mockOutput);
        Mockito.when(mockClock.millis()).thenReturn(1200L).thenReturn(1200L);
        generator.onEvent(1000L, 1000L, mockOutput);
        generator.onPeriodicEmit(mockOutput);
        Mockito.when(mockClock.millis()).thenReturn(1500L).thenReturn(1500L);
        generator.onEvent(1500L, 1500L, mockOutput);
        generator.onPeriodicEmit(mockOutput);

        // No more events.
        Mockito.when(mockClock.millis())
            .thenReturn(1700L)
            .thenReturn(2500L)
            .thenReturn(2501L)
            .thenReturn(3500L)
            .thenReturn(3502L)
            .thenReturn(3900L)
            .thenReturn(4000L)
            .thenReturn(4300L)
            .thenReturn(1000000L);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);
        generator.onPeriodicEmit(mockOutput);

        Mockito.verify(mockClock, Mockito.times(18)).millis();
        Mockito.verify(mockOutput, Mockito.times(14)).emitWatermark(capturedWatermarks.capture());
        Mockito.verifyNoMoreInteractions(mockOutput);

        Assertions.assertIterableEquals(
                FluentIterable.of(
                    // Event-based watermarks.
                    new Watermark(89),
                    new Watermark(489),
                    new Watermark(989),
                    new Watermark(1489),
                    // Periodic watermarks.
                    new Watermark(1489),
                    new Watermark(1489),
                    new Watermark(1489),
                    new Watermark(2499),
                    new Watermark(2501),
                    new Watermark(2899),
                    new Watermark(2999),
                    new Watermark(3299),
                    new Watermark(998999),
                    new Watermark(998999)),
                capturedWatermarks.getAllValues());
    }

    @Test
    public void singleBurst_sameTime() throws Exception {
        Mockito.when(mockClock.millis()).thenReturn(10L).thenReturn(15L);
        generator.onEvent(100L, 100L, mockOutput);
        generator.onPeriodicEmit(mockOutput);
        Mockito.when(mockClock.millis()).thenReturn(500L).thenReturn(550L);
        generator.onEvent(100L, 100L, mockOutput);
        generator.onPeriodicEmit(mockOutput);
        Mockito.when(mockClock.millis()).thenReturn(1200L).thenReturn(1200L);
        generator.onEvent(100L, 100L, mockOutput);
        generator.onPeriodicEmit(mockOutput);
        Mockito.when(mockClock.millis()).thenReturn(1500L).thenReturn(1500L);
        generator.onEvent(100L, 100L, mockOutput);
        generator.onPeriodicEmit(mockOutput);
        Mockito.when(mockClock.millis()).thenReturn(2499L).thenReturn(2499L);
        generator.onEvent(100L, 100L, mockOutput);
        generator.onPeriodicEmit(mockOutput);

        Mockito.verify(mockClock, Mockito.times(10)).millis();
        Mockito.verify(mockOutput, Mockito.times(5)).emitWatermark(capturedWatermarks.capture());
        Mockito.verifyNoMoreInteractions(mockOutput);

        Assertions.assertIterableEquals(
                FluentIterable.of(
                    // Event-based watermarks.
                    new Watermark(89),
                    new Watermark(89),
                    new Watermark(89),
                    new Watermark(89),
                    new Watermark(89)),
                capturedWatermarks.getAllValues());
    }
}
