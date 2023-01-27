package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.logprocessor.common.util.TinyFlatUtil;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyEvent;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.typeinfo.TypeInformation;

// TODO: consider adding something for inference.
public class ViewResponseInsertionProcessFunction extends BaseInferred<TinyDeliveryLog> {
    public ViewResponseInsertionProcessFunction(Options options) {
        // TODO - should this be "view x deliveryLog"?
        super("view x insertion",
                (TinyEvent event) -> !event.getViewId().isEmpty(),
                TinyDeliveryLog::getRequestId,
                TinyDeliveryLog::getViewId,
                TinyDeliveryLog::getLogTimestamp,
                TinyFlatUtil::createTinyFlatResponseInsertions,
                // Use the logUserId as a backup scope in case Request does not have a View.
                true,
                options);
    }

    protected ImmutableList<String> getLeftJoinIds(TinyEvent flat) {
        return ImmutableList.of(flat.getViewId());
    }

    protected ImmutableList<String> getRightJoinIds(TinyDeliveryLog deliveryLog) {
        return ImmutableList.of(deliveryLog.getViewId());
    }

    protected TypeInformation<TinyDeliveryLog> getRightTypeInfo() {
        return TypeInformation.of(TinyDeliveryLog.class);
    }

    protected boolean debugIdsMatch(TinyDeliveryLog rhs) {
        return options.debugIds().matches(rhs);
    }

    @Override
    protected TinyDeliveryLog setLogTimestamp(TinyDeliveryLog deliveryLog, long timestamp) {
        // We need to match the rightLogTimeGetter.
        TinyDeliveryLog.Builder deliveryLogBuilder = deliveryLog.toBuilder();
        deliveryLogBuilder.setLogTimestamp(timestamp);
        return deliveryLogBuilder.build();
    }
}
