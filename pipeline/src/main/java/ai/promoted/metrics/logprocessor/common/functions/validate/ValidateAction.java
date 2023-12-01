package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.ErrorType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.event.Action;
import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Filter Action records to valid ones. */
public class ValidateAction extends BaseValidate<Action> {
  public ValidateAction(boolean requireAnonUserId) {
    super(Action.class, requireAnonUserId);
  }

  @Override
  public void processElement(
      Action action, ProcessFunction<Action, Action>.Context ctx, Collector<Action> out)
      throws Exception {
    ImmutableList.Builder<ValidationError> errors = ImmutableList.builder();
    validateAnonUserId(action, action.getUserInfo(), errors);
    if (action.getImpressionId().isEmpty()
        && action.getInsertionId().isEmpty()
        && action.getContentId().isEmpty()
        && action.getCart().getContentsList().stream()
            .allMatch(cartContent -> cartContent.getContentId().isEmpty())) {
      errors.add(createError(action, ErrorType.MISSING_JOINABLE_ID, Field.MULTIPLE));
    }
    if (action.hasCart()) {
      // TODO - support warnings and switch this to a warning.
      if (action.getCart().getContentsList().stream()
          .anyMatch(content -> content.getQuantity() == 0L)) {
        errors.add(createError(action, ErrorType.MISSING_FIELD, Field.CART_CONTENT_QUANTITY));
      }
      // TODO(PRO-3137) - validate all CartContents have contentId.
      if (action.getCart().getContentsList().stream()
          .anyMatch(content -> content.getContentId().isEmpty())) {
        errors.add(createError(action, ErrorType.MISSING_FIELD, Field.CART_CONTENT_ID));
      }
    }
    outputErrorsOrRecord(action, errors, ctx, out);
  }

  @Override
  protected ValidationError.Builder createBaseErrorBuilder(Action action) {
    return ValidationError.newBuilder()
        .setRecordType(RecordType.ACTION)
        .setPlatformId(action.getPlatformId())
        .setAnonUserId(action.getUserInfo().getAnonUserId())
        .setViewId(action.getViewId())
        .setRequestId(action.getRequestId())
        .setResponseInsertionId(action.getInsertionId())
        .setImpressionId(action.getImpressionId())
        .setActionId(action.getActionId())
        .setTiming(toAvro(action.getTiming()));
  }
}
