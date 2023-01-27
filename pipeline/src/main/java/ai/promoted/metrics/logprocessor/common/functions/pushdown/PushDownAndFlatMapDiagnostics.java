package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.LogRequest;
import org.apache.flink.util.Collector;

import java.util.function.Consumer;

/**
 * Copies {@link LogRequest}'s {@link Diagnostics} and fills in default base values.
 * See base class for more details.
 */
public class PushDownAndFlatMapDiagnostics extends BasePushDownAndFlatMap<Diagnostics> {

    @Override
    public void flatMap(LogRequest logRequest, Collector<Diagnostics> out) {
        flatMap(logRequest, out::collect);
    }

    public void flatMap(LogRequest logRequest, Consumer<Diagnostics> out) {
        // Do this across all logUserIds.
        String lowerCaseBatchLogUserId = logRequest.getUserInfo().getLogUserId().toLowerCase();
        logRequest.getDiagnosticsList().stream()
                .map(diagnostics -> pushDownFields(diagnostics, logRequest, lowerCaseBatchLogUserId))
                .forEach(out);
    }

    public static Diagnostics pushDownFields(Diagnostics diagnostics, LogRequest batchValue, String lowerCaseBatchLogUserId) {
        Diagnostics.Builder builder = diagnostics.toBuilder();
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
