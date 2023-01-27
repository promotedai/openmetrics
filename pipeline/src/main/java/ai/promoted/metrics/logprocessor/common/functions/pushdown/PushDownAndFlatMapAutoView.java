package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.LogRequest;
import org.apache.flink.util.Collector;

import java.util.function.Consumer;

/**
 * Copies {@link LogRequest}'s {@link AutoView} and fills in default base values.
 * See base class for more details.
 */
public class PushDownAndFlatMapAutoView extends BasePushDownAndFlatMap<AutoView> {

    @Override
    public void flatMap(LogRequest logRequest, Collector<AutoView> out) {
        flatMap(logRequest, out::collect);
    }

    public void flatMap(LogRequest logRequest, Consumer<AutoView> out) {
        // Do this across all logUserIds.
        String lowerCaseBatchLogUserId = logRequest.getUserInfo().getLogUserId().toLowerCase();
        logRequest.getAutoViewList().stream()
                .map(view -> pushDownFields(view, logRequest, lowerCaseBatchLogUserId))
                .forEach(out);
    }

    public static AutoView pushDownFields(AutoView autoView, LogRequest batchValue, String lowerCaseBatchLogUserId) {
        AutoView.Builder builder = autoView.toBuilder();
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
        return builder.build();
    }
}
