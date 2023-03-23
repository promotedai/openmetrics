package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.ErrorType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Filter DeliveryLog records to valid ones. */
public class ValidateDeliveryLog extends BaseValidate<DeliveryLog> {
  public void processElement(
      DeliveryLog deliveryLog,
      ProcessFunction<DeliveryLog, DeliveryLog>.Context ctx,
      Collector<DeliveryLog> out)
      throws Exception {
    ImmutableList.Builder<ValidationError> errors = ImmutableList.builder();
    if (deliveryLog.getRequest().getUserInfo().getLogUserId().isEmpty()) {
      errors.add(createError(deliveryLog, ErrorType.MISSING_FIELD, Field.LOG_USER_ID));
    }
    if (hasMismatchedMatrixHeaderLength(deliveryLog)) {
      errors.add(
          createError(deliveryLog, ErrorType.MISMATCHED_MATRIX_HEADER_LENGTH, Field.MULTIPLE));
    }
    outputErrorsOrRecord(deliveryLog, errors, ctx, out);
  }

  protected boolean hasMismatchedMatrixHeaderLength(DeliveryLog deliveryLog) {
    var request = deliveryLog.getRequest();
    for (var insertion : request.getInsertionMatrix().getValuesList()) {
      if (insertion.getListValue().getValuesCount() != request.getInsertionMatrixHeadersCount()) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected ValidationError.Builder createBaseErrorBuilder(DeliveryLog deliveryLog) {
    Request request = deliveryLog.getRequest();
    return ValidationError.newBuilder()
        .setRecordType(RecordType.DELIVERY_LOG)
        .setPlatformId(request.getPlatformId())
        .setLogUserId(request.getUserInfo().getLogUserId())
        .setViewId(request.getViewId())
        .setRequestId(request.getRequestId())
        .setTiming(toAvro(request.getTiming()));
  }
}
