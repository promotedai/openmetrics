package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.User;
import org.apache.flink.util.Collector;

import java.util.function.Consumer;

/**
 * Copies {@link LogRequest}'s {@link User} and fills in default base values.
 * See base class for more details.
 */
public class PushDownAndFlatMapUser extends BasePushDownAndFlatMap<User> {

    @Override
    public void flatMap(LogRequest logRequest, Collector<User> out) {
        flatMap(logRequest, out::collect);
    }

    public void flatMap(LogRequest logRequest, Consumer<User> out) {
        // Do this across all logUserIds.
        String lowerCaseBatchLogUserId = logRequest.getUserInfo().getLogUserId().toLowerCase();
        logRequest.getUserList().stream()
                .map(user -> pushDownFields(user, logRequest, lowerCaseBatchLogUserId))
                .forEach(out);
    }

    public static User pushDownFields(User user, LogRequest batchValue, String lowerCaseBatchLogUserId) {
        User.Builder builder = user.toBuilder();
        if (builder.getPlatformId() == 0) {
            builder.setPlatformId(batchValue.getPlatformId());
        }
        if (batchValue.hasUserInfo()) {
            pushDownUserIdFields(builder.getUserInfoBuilder(), batchValue);
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
