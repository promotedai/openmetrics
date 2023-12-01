package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.metrics.common.JoinedIdentifiers;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.LogFunctionName;
import ai.promoted.metrics.error.MismatchError;
import com.google.auto.value.AutoValue;
import java.util.function.Consumer;

/** Options for creating {@link JoinValueSetter}. */
@AutoValue
public abstract class JoinValueSetterOptions {
  public static JoinValueSetterOptions.Builder builder() {
    return new AutoValue_JoinValueSetterOptions.Builder();
  }

  public abstract RecordType recordType();

  public abstract String recordId();

  public abstract long eventApiTimestamp();

  public abstract JoinedIdentifiers lhsIds();

  public abstract LogFunctionName logFunctionName();

  // We use this type so we can directly pass around the Context's instance method and not have to
  // wrap it.
  // This avoids a bunch of unnecessary interface wrapping per record per process function.
  public abstract Consumer<MismatchError> errorLogger();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract JoinValueSetterOptions.Builder setRecordType(RecordType recordType);

    public abstract JoinValueSetterOptions.Builder setRecordId(String recordId);

    public abstract JoinValueSetterOptions.Builder setEventApiTimestamp(long eventApiTimestamp);

    public abstract JoinValueSetterOptions.Builder setLhsIds(JoinedIdentifiers lhsIds);

    public abstract JoinValueSetterOptions.Builder setLogFunctionName(
        LogFunctionName logFunctionName);

    public abstract JoinValueSetterOptions.Builder setErrorLogger(
        Consumer<MismatchError> errorLogger);

    public abstract JoinValueSetterOptions build();
  }
}
