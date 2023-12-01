package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.event.Diagnostics;
import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Filter Diagnostics records to valid ones. */
public class ValidateDiagnostics extends BaseValidate<Diagnostics> {
  public ValidateDiagnostics(boolean requireAnonUserId) {
    super(Diagnostics.class, requireAnonUserId);
  }

  @Override
  public void processElement(
      Diagnostics diagnostics,
      ProcessFunction<Diagnostics, Diagnostics>.Context ctx,
      Collector<Diagnostics> out)
      throws Exception {
    ImmutableList.Builder<ValidationError> errors = ImmutableList.builder();
    validateAnonUserId(diagnostics, diagnostics.getUserInfo(), errors);
    outputErrorsOrRecord(diagnostics, errors, ctx, out);
  }

  @Override
  protected ValidationError.Builder createBaseErrorBuilder(Diagnostics diagnostics) {
    return ValidationError.newBuilder()
        .setRecordType(RecordType.DIAGNOSTICS)
        .setPlatformId(diagnostics.getPlatformId())
        .setAnonUserId(diagnostics.getUserInfo().getAnonUserId())
        // No diagnosticsId.
        .setTiming(toAvro(diagnostics.getTiming()));
  }
}
