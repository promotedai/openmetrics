package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.LogFunctionName;
import ai.promoted.metrics.error.MismatchError;
import ai.promoted.proto.event.JoinedIdentifiers;
import com.google.common.collect.ImmutableSet;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class JoinValueSetterTest {

    private JoinValueSetterOptions options;
    private JoinValueSetter setter;
    private BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger;

    @BeforeEach
    public void setUp() {
        errorLogger = Mockito.mock(BiConsumer.class);
        options = JoinValueSetterOptions.builder()
                .setRecordType(RecordType.IMPRESSION)
                .setRecordId("imp1")
                .setLogTimestamp(123L)
                .setLhsIds(ai.promoted.metrics.common.JoinedIdentifiers.newBuilder()
                        .setPlatformId(1L)
                        .setViewId("view1")
                        .setRequestId("req1")
                        .setInsertionId("ins1")
                        .build())
                .setLogFunctionName(LogFunctionName.FLAT_UTIL_SET_FLAT_IMPRESSION)
                .setErrorLogger(errorLogger)
                .build();
        setter = new JoinValueSetter(options);
    }

    @Test
    public void toAvro_joinedIdentifiers() {
        assertEquals(
                ai.promoted.metrics.common.JoinedIdentifiers.newBuilder()
                        .setPlatformId(1L)
                        .setViewId("view1")
                        .setRequestId("req1")
                        .setInsertionId("ins1")
                        .build(),
                JoinValueSetter.toAvro(JoinedIdentifiers.newBuilder()
                        .setPlatformId(1L)
                        .setViewId("view1")
                        .setRequestId("req1")
                        .setInsertionId("ins1")
                        .build()));
    }

    @Test
    public void toAvro_catchNewFields() {
        assertEquals(9,
            JoinedIdentifiers.getDescriptor().getFields().stream().map(field -> field.getNumber()).collect(Collectors.toSet())
                .size());
    }

    // Longs.

    @Test
    public void setValue_long() {
        Function<Long, Void> setterFn = Mockito.mock(Function.class);
        setter.setValue(Field.PLATFORM_ID, setterFn, 0L, 1L);
        verify(setterFn).apply(1L);
        verify(errorLogger, never()).accept(any(), any());
    }

    @Test
    public void setValue_long_sameValue() {
        Function<Long, Void> setterFn = Mockito.mock(Function.class);
        setter.setValue(Field.PLATFORM_ID, setterFn, 1L, 1L);
        verify(setterFn, never()).apply(any());
        verify(errorLogger, never()).accept(any(), any());
    }

    @Test
    public void setValue_long_mismatch() {
        Function<Long, Void> setterFn = Mockito.mock(Function.class);
        setter.setValue(Field.PLATFORM_ID, setterFn, 1L, 2L);
        verify(setterFn, never()).apply(any());
        verify(errorLogger).accept(any(), eq(MismatchError.newBuilder()
                .setRecordType(RecordType.IMPRESSION)
                .setField(Field.PLATFORM_ID)
                .setLhsIds(ai.promoted.metrics.common.JoinedIdentifiers.newBuilder()
                        .setPlatformId(1L)
                        .setViewId("view1")
                        .setRequestId("req1")
                        .setInsertionId("ins1")
                        .build())
                .setRhsRecordId("imp1")
                .setLhsLong(1L)
                .setRhsLong(2L)
                .setLogTimestamp(123L)
                .setLogFunctionName(LogFunctionName.FLAT_UTIL_SET_FLAT_IMPRESSION)
                .build()));
    }

    @Test
    public void setValue_long_noLogMismatch() {
        Function<Long, Void> setterFn = Mockito.mock(Function.class);
        setter.setValue(Field.PLATFORM_ID, setterFn, 1L, 2L, false);
        verify(setterFn, never()).apply(any());
        verify(errorLogger, never()).accept(any(), any());
    }

    // Strings.

    @Test
    public void setValue_string() {
        Function<String, Void> setterFn = Mockito.mock(Function.class);
        setter.setValue(Field.VIEW_ID, setterFn, "", "view1");
        verify(setterFn).apply("view1");
        verify(errorLogger, never()).accept(any(), any());
    }

    @Test
    public void setValue_string_sameValue() {
        Function<String, Void> setterFn = Mockito.mock(Function.class);
        setter.setValue(Field.VIEW_ID, setterFn, "view1", "view1");
        verify(setterFn, never()).apply(any());
        verify(errorLogger, never()).accept(any(), any());
    }

    @Test
    public void setValue_string_mismatch() {
        Function<String, Void> setterFn = Mockito.mock(Function.class);
        setter.setValue(Field.VIEW_ID, setterFn, "view1", "view2");
        verify(setterFn, never()).apply(any());
        verify(errorLogger).accept(any(), eq(MismatchError.newBuilder()
                .setRecordType(RecordType.IMPRESSION)
                .setField(Field.VIEW_ID)
                .setLhsIds(ai.promoted.metrics.common.JoinedIdentifiers.newBuilder()
                        .setPlatformId(1L)
                        .setViewId("view1")
                        .setRequestId("req1")
                        .setInsertionId("ins1")
                        .build())
                .setRhsRecordId("imp1")
                .setLhsString("view1")
                .setRhsString("view2")
                .setLogTimestamp(123L)
                .setLogFunctionName(LogFunctionName.FLAT_UTIL_SET_FLAT_IMPRESSION)
                .build()));
    }

    @Test
    public void setValue_string_noLogMismatch() {
        Function<String, Void> setterFn = Mockito.mock(Function.class);
        setter.setValue(Field.VIEW_ID, setterFn, "view1", "view2", false);
        verify(setterFn, never()).apply(any());
        verify(errorLogger, never()).accept(any(), any());
    }
}
