package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.ErrorType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.event.User;
import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Filter User records to valid ones.
 * https://toil.kitemaker.co/se6ONh-Promoted/hdRZPv-Promoted/items/836
 */
public class ValidateUser extends BaseValidate<User> {

  @Override
  public void processElement(
      User user, ProcessFunction<User, User>.Context ctx, Collector<User> out) throws Exception {
    ImmutableList.Builder<ValidationError> errors = ImmutableList.builder();
    if (user.getUserInfo().getUserId().isEmpty()) {
      errors.add(createError(user, ErrorType.MISSING_FIELD, Field.USER_ID));
    }
    outputErrorsOrRecord(user, errors, ctx, out);
  }

  @Override
  protected ValidationError.Builder createBaseErrorBuilder(User user) {
    return ValidationError.newBuilder()
        .setRecordType(RecordType.USER)
        .setPlatformId(user.getPlatformId())
        .setLogUserId(user.getUserInfo().getLogUserId())
        .setTiming(toAvro(user.getTiming()));
  }
}
