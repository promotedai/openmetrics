package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.error.ErrorType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.common.Timing;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Base validation function. */
public abstract class BaseValidate<T> extends ProcessFunction<T, T> {

  public static final OutputTag<ValidationError> INVALID_TAG =
      new OutputTag<ValidationError>("invalid") {};

  protected static <T> void outputErrorsOrRecord(
      T record,
      ImmutableList.Builder<ValidationError> errors,
      ProcessFunction<T, T>.Context ctx,
      Collector<T> out)
      throws Exception {
    outputErrorsOrRecord(record, errors.build(), ctx, out);
  }

  protected static <T> void outputErrorsOrRecord(
      T record, List<ValidationError> errors, ProcessFunction<T, T>.Context ctx, Collector<T> out)
      throws Exception {
    if (errors.isEmpty()) {
      out.collect(record);
    } else {
      errors.forEach(error -> ctx.output(INVALID_TAG, error));
    }
  }

  protected static ai.promoted.metrics.common.Timing toAvro(Timing timing) {
    return ai.promoted.metrics.common.Timing.newBuilder()
        .setClientLogTimestamp(timing.getClientLogTimestamp())
        .setEventApiTimestamp(timing.getEventApiTimestamp())
        .setLogTimestamp(timing.getLogTimestamp())
        .build();
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
