package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.Session;
import ai.promoted.proto.event.SessionProfile;
import org.apache.flink.util.Collector;

import java.util.function.Consumer;

/**
 * Copies {@link LogRequest}'s {@link SessionProfile} and fills in default base values.
 * See base class for more details.
 */
public class PushDownAndFlatMapSessionProfile extends BasePushDownAndFlatMap<SessionProfile> {

    @Override
    public void flatMap(LogRequest logRequest, Collector<SessionProfile> out) {
        flatMap(logRequest, out::collect);
    }

    public void flatMap(LogRequest logRequest, Consumer<SessionProfile> out) {
        logRequest.getSessionProfileList().stream()
                .map(sessionProfile -> pushDownFields(sessionProfile, logRequest))
                .forEach(out);
    }

    private static SessionProfile pushDownFields(SessionProfile sessionProfile, LogRequest batchValue) {
        SessionProfile.Builder builder = sessionProfile.toBuilder();
        if (builder.getPlatformId() == 0) {
            builder.setPlatformId(batchValue.getPlatformId());
        }
        if (batchValue.hasUserInfo()) {
            pushDownUserIdFields(builder.getUserInfoBuilder(), batchValue);
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
