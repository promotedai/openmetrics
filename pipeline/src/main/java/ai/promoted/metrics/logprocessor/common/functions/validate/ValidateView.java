package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.ErrorType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.event.View;
import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Filter View records to valid ones. */
public class ValidateView extends BaseValidate<View> {

  @Override
  public void processElement(
      View view, ProcessFunction<View, View>.Context ctx, Collector<View> out) throws Exception {
    ImmutableList.Builder<ValidationError> errors = ImmutableList.builder();
    if (view.getUserInfo().getLogUserId().isEmpty()) {
      errors.add(createError(view, ErrorType.MISSING_FIELD, Field.LOG_USER_ID));
    }
    outputErrorsOrRecord(view, errors, ctx, out);
  }

  @Override
  protected ValidationError.Builder createBaseErrorBuilder(View view) {
    return ValidationError.newBuilder()
        .setRecordType(RecordType.VIEW)
        .setPlatformId(view.getPlatformId())
        .setLogUserId(view.getUserInfo().getLogUserId())
        .setViewId(view.getViewId())
        .setTiming(toAvro(view.getTiming()));
  }
}
