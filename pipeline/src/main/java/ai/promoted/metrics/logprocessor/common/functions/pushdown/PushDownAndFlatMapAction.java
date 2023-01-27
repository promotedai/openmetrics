package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.LogRequest;
import org.apache.flink.util.Collector;

import java.util.function.Consumer;

/**
 * Copies {@link LogRequest}'s {@link Action} and fills in default base values.
 * See base class for more details.
 */
public class PushDownAndFlatMapAction extends BasePushDownAndFlatMap<Action> {

    @Override
    public void flatMap(LogRequest logRequest, Collector<Action> out) {
        flatMap(logRequest, out::collect);
    }

    public void flatMap(LogRequest logRequest, Consumer<Action> out) {
        // Do this across all logUserIds.
        String lowerCaseBatchLogUserId = logRequest.getUserInfo().getLogUserId().toLowerCase();
        logRequest.getActionList().stream()
                .map(action -> pushDownFields(action, logRequest, lowerCaseBatchLogUserId))
                .forEach(out);
    }

    public static Action pushDownFields(Action action, LogRequest batchValue, String lowerCaseBatchLogUserId) {
        Action.Builder builder = action.toBuilder();
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
        if (batchValue.hasDevice()) {
            pushDownDevice(builder.getDeviceBuilder(), batchValue);
        }
        return builder.build();
    }
}
