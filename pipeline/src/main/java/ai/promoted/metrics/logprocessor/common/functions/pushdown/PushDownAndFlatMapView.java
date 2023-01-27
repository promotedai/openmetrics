package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.View;
import org.apache.flink.util.Collector;

import java.util.function.Consumer;

/**
 * Copies {@link LogRequest}'s {@link View} and fills in default base values.
 * See base class for more details.
 */
public class PushDownAndFlatMapView extends BasePushDownAndFlatMap<View> {

    @Override
    public void flatMap(LogRequest logRequest, Collector<View> out) {
        flatMap(logRequest, out::collect);
    }

    public void flatMap(LogRequest logRequest, Consumer<View> out) {
        // Do this across all logUserIds.
        String lowerCaseBatchLogUserId = logRequest.getUserInfo().getLogUserId().toLowerCase();
        logRequest.getViewList().stream()
                .map(view -> pushDownFields(view, logRequest, lowerCaseBatchLogUserId))
                .forEach(out);
    }

    public static View pushDownFields(View view, LogRequest batchValue, String lowerCaseBatchLogUserId) {
        View.Builder builder = view.toBuilder();
        if (builder.getPlatformId() == 0) {
            builder.setPlatformId(batchValue.getPlatformId());
        }
        if (batchValue.hasUserInfo()) {
            pushDownLogUserIdFields(builder.getUserInfoBuilder(), lowerCaseBatchLogUserId);
        } else if (builder.hasUserInfo()) {
            // If the top-level `LogRequest.user_info` is not set, we still want to lowercase any logUserIds.
            lowerCaseLogUserIdFields(builder.getUserInfoBuilder());
        }
        if (batchValue.hasTiming()) {
            pushDownTiming(builder.getTimingBuilder(), batchValue);
        }
        if (batchValue.hasClientInfo()) {
            pushDownClientInfo(builder.getClientInfoBuilder(), batchValue);
        }
        /* Marked as deprecated, but used still... should we do this?
        if (batchValue.hasDevice()) {
            pushDownDevice(builder.getDeviceBuilder(), batchValue);
        }*/
        return builder.build();
    }
}
