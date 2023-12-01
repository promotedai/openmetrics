package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.event.CohortMembership;
import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Filter CohortMembership records to valid ones. */
public class ValidateCohortMembership extends BaseValidate<CohortMembership> {
  public ValidateCohortMembership(boolean requireAnonUserId) {
    super(CohortMembership.class, requireAnonUserId);
  }

  @Override
  public void processElement(
      CohortMembership cohortMembership,
      ProcessFunction<CohortMembership, CohortMembership>.Context ctx,
      Collector<CohortMembership> out)
      throws Exception {
    ImmutableList.Builder<ValidationError> errors = ImmutableList.builder();
    validateAnonUserId(cohortMembership, cohortMembership.getUserInfo(), errors);
    outputErrorsOrRecord(cohortMembership, errors, ctx, out);
  }

  @Override
  protected ValidationError.Builder createBaseErrorBuilder(CohortMembership cohortMembership) {
    return ValidationError.newBuilder()
        .setRecordType(RecordType.COHORT_MEMBERSHIP)
        .setPlatformId(cohortMembership.getPlatformId())
        .setAnonUserId(cohortMembership.getUserInfo().getAnonUserId())
        .setCohortMembershipId(cohortMembership.getMembershipId())
        .setTiming(toAvro(cohortMembership.getTiming()));
  }
}
