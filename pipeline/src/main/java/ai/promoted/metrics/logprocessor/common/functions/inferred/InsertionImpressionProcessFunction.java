package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.logprocessor.common.util.TinyFlatUtil;
import ai.promoted.proto.event.TinyEvent;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class InsertionImpressionProcessFunction extends ContentId<TinyEvent> {
    public InsertionImpressionProcessFunction(Options options) {
        super("insertion x impression",
                (TinyEvent event) -> !event.getInsertionId().isEmpty(),
                TinyFlatUtil::getAllContentIds,
                TinyEvent::getImpressionId,
                TinyEvent::getInsertionId,
                TinyFlatUtil::getAllContentIds,
                TinyEvent::getLogTimestamp,
                TinyFlatUtil::mergeImpression,
                false,
                options);
    }

    protected ImmutableList<String> getLeftJoinIds(TinyEvent flat) {
        return ImmutableList.of(
                flat.getInsertionId(),
                flat.getRequestId(),
                flat.getViewId(),
                flat.getContentId());
    }

    protected ImmutableList<String> getRightJoinIds(TinyEvent imp) {
        return ImmutableList.of(
            imp.getInsertionId(),
            imp.getRequestId(),
            imp.getViewId(),
            // CodeReview - Should we allow ContentID here?
            imp.getContentId());
    }

    protected TypeInformation<TinyEvent> getRightTypeInfo() {
        return TypeInformation.of(TinyEvent.class);
    }

    protected boolean debugIdsMatch(TinyEvent rhs) {
        return options.debugIds().matches(rhs);
    }

    protected TinyEvent setLogTimestamp(TinyEvent imp, long timestamp) {
        TinyEvent.Builder builder = imp.toBuilder();
        builder.setLogTimestamp(timestamp);
        return builder.build();
    }
}
