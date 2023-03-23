package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.metrics.common.JoinedIdentifiers;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.LogFunctionName;
import ai.promoted.metrics.error.MismatchError;
import com.google.auto.value.AutoValue;
import java.util.function.BiConsumer;
import org.apache.flink.util.OutputTag;

/** Options for creating {@link JoinValueSetter}. */
@AutoValue
public abstract class JoinValueSetterOptions {
  public abstract RecordType recordType();

  public abstract String recordId();

  public abstract long logTimestamp();

  public abstract JoinedIdentifiers lhsIds();

  public abstract LogFunctionName logFunctionName();
  // We use this type so we can directly pass around the Context's instance method and not have to
  // wrap it.
  // This avoids a bunch of unnecessary interface wrapping per record per process function.
  public abstract BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger();

  public static JoinValueSetterOptions.Builder builder() {
    return new AutoValue_JoinValueSetterOptions.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract JoinValueSetterOptions.Builder setRecordType(RecordType recordType);

    public abstract JoinValueSetterOptions.Builder setRecordId(String recordId);

    public abstract JoinValueSetterOptions.Builder setLogTimestamp(long logTimestamp);

    public abstract JoinValueSetterOptions.Builder setLhsIds(JoinedIdentifiers lhsIds);

    public abstract JoinValueSetterOptions.Builder setLogFunctionName(
        LogFunctionName logFunctionName);

    public abstract JoinValueSetterOptions.Builder setErrorLogger(
        BiConsumer<OutputTag<MismatchError>, MismatchError> errorLogger);

    public abstract JoinValueSetterOptions build();
  }
}
