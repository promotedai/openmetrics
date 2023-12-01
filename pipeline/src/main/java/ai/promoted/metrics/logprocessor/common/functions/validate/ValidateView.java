package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.event.View;
import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Filter View records to valid ones. */
public class ValidateView extends BaseValidate<View> {
  public ValidateView(boolean requireAnonUserId) {
    super(View.class, requireAnonUserId);
  }

  @Override
  public void processElement(
      View view, ProcessFunction<View, View>.Context ctx, Collector<View> out) throws Exception {
    ImmutableList.Builder<ValidationError> errors = ImmutableList.builder();
    validateAnonUserId(view, view.getUserInfo(), errors);
    outputErrorsOrRecord(view, errors, ctx, out);
  }

  @Override
  protected ValidationError.Builder createBaseErrorBuilder(View view) {
    return ValidationError.newBuilder()
        .setRecordType(RecordType.VIEW)
        .setPlatformId(view.getPlatformId())
        .setAnonUserId(view.getUserInfo().getAnonUserId())
        .setViewId(view.getViewId())
        .setTiming(toAvro(view.getTiming()));
  }
}
