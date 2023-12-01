package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.ErrorType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.event.Impression;
import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Filter Impression records to valid ones. */
public class ValidateImpression extends BaseValidate<Impression> {
  public ValidateImpression(boolean requireAnonUserId) {
    super(Impression.class, requireAnonUserId);
  }

  public void processElement(
      Impression impression,
      ProcessFunction<Impression, Impression>.Context ctx,
      Collector<Impression> out)
      throws Exception {
    ImmutableList.Builder<ValidationError> errors = ImmutableList.builder();
    validateAnonUserId(impression, impression.getUserInfo(), errors);
    if (impression.getInsertionId().isEmpty() && impression.getContentId().isEmpty()) {
      errors.add(createError(impression, ErrorType.MISSING_JOINABLE_ID, Field.MULTIPLE));
    }
    outputErrorsOrRecord(impression, errors, ctx, out);
  }

  @Override
  protected ValidationError.Builder createBaseErrorBuilder(Impression impression) {
    return ValidationError.newBuilder()
        .setRecordType(RecordType.IMPRESSION)
        .setPlatformId(impression.getPlatformId())
        .setAnonUserId(impression.getUserInfo().getAnonUserId())
        .setViewId(impression.getViewId())
        .setRequestId(impression.getRequestId())
        .setResponseInsertionId(impression.getInsertionId())
        .setImpressionId(impression.getImpressionId())
        .setTiming(toAvro(impression.getTiming()));
  }
}
