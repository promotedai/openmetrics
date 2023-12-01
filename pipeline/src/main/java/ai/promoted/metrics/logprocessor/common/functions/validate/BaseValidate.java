package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.error.ErrorType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Base validation function. */
public abstract class BaseValidate<T> extends ProcessFunction<T, T> {

  public static final OutputTag<ValidationError> VALIDATION_ERROR_TAG =
      new OutputTag<>("validation-error") {};

  private final OutputTag<T> invalidRecordTag;

  private final boolean requireAnonUserId;

  protected BaseValidate(Class<T> clazz, boolean requireAnonUserId) {
    this.invalidRecordTag = new OutputTag<>("invalid-record", TypeInformation.of(clazz)) {};
    this.requireAnonUserId = requireAnonUserId;
  }

  protected static ai.promoted.metrics.common.Timing toAvro(Timing timing) {
    return ai.promoted.metrics.common.Timing.newBuilder()
        .setClientLogTimestamp(timing.getClientLogTimestamp())
        .setEventApiTimestamp(timing.getEventApiTimestamp())
        .setLogTimestamp(timing.getLogTimestamp())
        .build();
  }

  public OutputTag<T> getInvalidRecordTag() {
    return invalidRecordTag;
  }

  protected void validateAnonUserId(
      T record, UserInfo userInfo, ImmutableList.Builder<ValidationError> errors) {
    if (requireAnonUserId && userInfo.getAnonUserId().isEmpty()) {
      errors.add(createError(record, ErrorType.MISSING_FIELD, Field.ANON_USER_ID));
    }
  }

  protected void outputErrorsOrRecord(
      T record,
      ImmutableList.Builder<ValidationError> errorsBuilder,
      ProcessFunction<T, T>.Context ctx,
      Collector<T> out)
      throws Exception {
    List<ValidationError> errors = errorsBuilder.build();
    if (errors.isEmpty()) {
      out.collect(record);
    } else {
      errors.forEach(error -> ctx.output(VALIDATION_ERROR_TAG, error));
      ctx.output(invalidRecordTag, record);
    }
  }

  /**
   * Creates a base {@link ValidationError.Builder} for the record by setting the fields specific to
   * the record. This is called by {@code createError} which fills in error specific fields.
   */
  protected abstract ValidationError.Builder createBaseErrorBuilder(T record);

  /** Creates field errors. */
  protected ValidationError createError(T record, ErrorType errorType, Field field) {
    return createBaseErrorBuilder(record).setErrorType(errorType).setField(field).build();
  }
}
