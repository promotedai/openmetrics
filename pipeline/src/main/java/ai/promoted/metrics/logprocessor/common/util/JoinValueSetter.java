package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.logprocessor.common.error.MismatchErrorTag;
import ai.promoted.proto.event.JoinedIdentifiers;

import java.util.function.Function;

/**
 * Merges common values together in our joins.  Handles logging side output when the values differ.
 */
public class JoinValueSetter {

    public static ai.promoted.metrics.common.JoinedIdentifiers toAvro(JoinedIdentifiers ids) {
        return ai.promoted.metrics.common.JoinedIdentifiers.newBuilder()
                .setPlatformId(ids.getPlatformId())
                .setLogUserId(ids.getLogUserId())
                .setSessionId(ids.getSessionId())
                .setViewId(ids.getViewId())
                .setAutoViewId(ids.getAutoViewId())
                .setRequestId(ids.getRequestId())
                .setInsertionId(ids.getInsertionId())
                .setImpressionId(ids.getImpressionId())
                .build();
    }

    private final JoinValueSetterOptions options;

    public JoinValueSetter(JoinValueSetterOptions options) {
        this.options = options;
    }

    public void setValue(
            Field field,
            Function<Long, ? extends Object> setter,
            long base,
            long newValue) {
        setValue(field, setter, base, newValue, true);
    }

    @SuppressWarnings("ReturnValueIgnored")
    public void setValue(
            Field field,
            Function<Long, ? extends Object> setter,
            long lhsValue,
            long rhsValue,
            boolean logMismatchedValues) {
        if (lhsValue == 0) {
            setter.apply(rhsValue);
        } else if (rhsValue != 0) {
            if (lhsValue != rhsValue && logMismatchedValues) {
                // Do not override the current value.  Assume the earlier join value is more correct.
                options.errorLogger().accept(
                        MismatchErrorTag.TAG,
                        MismatchError.newBuilder()
                                .setRecordType(options.recordType())
                                .setField(field)
                                .setLhsIds(options.lhsIds())
                                .setRhsRecordId(options.recordId())
                                .setLhsLong(lhsValue)
                                .setRhsLong(rhsValue)
                                .setLogTimestamp(options.logTimestamp())
                                .setLogFunctionName(options.logFunctionName())
                                .build());
            }
        }
    }

    public void setValue(
            Field field,
            Function<String, ? extends Object> setter,
            String base,
            String newValue) {
        setValue(field, setter, base, newValue, true);
    }

    @SuppressWarnings("ReturnValueIgnored")
    public void setValue(
            Field field,
            Function<String, ? extends Object> setter,
            String lhsValue,
            String rhsValue,
            boolean logMismatchedValues) {
        if (lhsValue.isEmpty()) {
            setter.apply(rhsValue);
        } else if (!rhsValue.isEmpty()) {
            if (!lhsValue.equals(rhsValue) && logMismatchedValues) {
                // Do not override the current value.  Assume the earlier join value is more correct.
                options.errorLogger().accept(
                        MismatchErrorTag.TAG,
                        MismatchError.newBuilder()
                                .setRecordType(options.recordType())
                                .setField(field)
                                .setLhsIds(options.lhsIds())
                                .setRhsRecordId(options.recordId())
                                .setLhsString(lhsValue)
                                .setRhsString(rhsValue)
                                .setLogTimestamp(options.logTimestamp())
                                .setLogFunctionName(options.logFunctionName())
                                .build());
            }
        }
    }
}