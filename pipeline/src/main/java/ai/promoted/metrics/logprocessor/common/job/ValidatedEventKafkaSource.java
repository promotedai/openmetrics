package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.proto.common.AnonUserRetainedUser;
import ai.promoted.proto.common.RetainedUser;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ValidatedEventKafkaSource implements FlinkSegment, ValidatedDataSourceProvider {
  public static final String VALIDATED_RO_DB_PREFIX = "validated_ro";
  private final DirectValidatedEventKafkaSource directValidatedEventKafkaSource;
  private final ValidatedEventKafkaSegment validatedEventKafkaSegment;
  private final KeepFirstSegment keepFirstSegment;

  public ValidatedEventKafkaSource(
      DirectValidatedEventKafkaSource directValidatedEventKafkaSource,
      ValidatedEventKafkaSegment validatedEventKafkaSegment,
      KeepFirstSegment keepFirstSegment) {
    this.directValidatedEventKafkaSource = directValidatedEventKafkaSource;
    this.validatedEventKafkaSegment = validatedEventKafkaSegment;
    this.keepFirstSegment = keepFirstSegment;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    // Do not register Segments passed into the constructor.
    return directValidatedEventKafkaSource.getProtoClasses();
  }

  public SingleOutputStreamOperator<RetainedUser> getRetainedUserSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getRetainedUserSource(env, consumerGroupId, KeyUtil.retainedUserKeySelector);
  }

  public SingleOutputStreamOperator<RetainedUser> getRetainedUserSource(
      StreamExecutionEnvironment env,
      String consumerGroupId,
      KeySelector<RetainedUser, ?> keySelector) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getRetainedUserSource(env, consumerGroupId),
        TypeInformation.of(RetainedUser.class),
        keySelector,
        validatedEventKafkaSegment.getRetainedUserTopics().getSourceTopic());
  }

  public SingleOutputStreamOperator<LogUserUser> getLogUserUserSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getLogUserUserSource(env, consumerGroupId, KeyUtil.logUserUserKeySelector);
  }

  public SingleOutputStreamOperator<LogUserUser> getLogUserUserSource(
      StreamExecutionEnvironment env,
      String consumerGroupId,
      KeySelector<LogUserUser, ?> keySelector) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getLogUserUserSource(env, consumerGroupId),
        TypeInformation.of(LogUserUser.class),
        keySelector,
        validatedEventKafkaSegment.getLogUserUserTopics().getSourceTopic());
  }

  public SingleOutputStreamOperator<AnonUserRetainedUser> getAnonUserRetainedUserSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getAnonUserRetainedUserSource(
        env, consumerGroupId, KeyUtil.anonUserRetainedUserKeySelector);
  }

  public SingleOutputStreamOperator<AnonUserRetainedUser> getAnonUserRetainedUserSource(
      StreamExecutionEnvironment env,
      String consumerGroupId,
      KeySelector<AnonUserRetainedUser, ?> keySelector) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getAnonUserRetainedUserSource(env, consumerGroupId),
        TypeInformation.of(AnonUserRetainedUser.class),
        keySelector,
        validatedEventKafkaSegment.getAnonUserRetainedUserTopics().getSourceTopic());
  }

  public SingleOutputStreamOperator<ValidationError> getValidationErrorSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getValidationErrorSource(env, consumerGroupId, KeyUtil.validationErrorKeySelector);
  }

  public SingleOutputStreamOperator<ValidationError> getValidationErrorSource(
      StreamExecutionEnvironment env,
      String consumerGroupId,
      KeySelector<ValidationError, ?> keySelector) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getValidationErrorSource(env, consumerGroupId),
        TypeInformation.of(ValidationError.class),
        keySelector,
        validatedEventKafkaSegment.getValidationErrorTopics().getSourceTopic());
  }

  public SingleOutputStreamOperator<CohortMembership> getCohortMembershipSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getCohortMembershipSource(env, consumerGroupId),
        TypeInformation.of(CohortMembership.class),
        KeyUtil.cohortMembershipKeySelector,
        validatedEventKafkaSegment.getCohortMembershipTopics().getValid().getSourceTopic());
  }

  public SingleOutputStreamOperator<CohortMembership> getInvalidCohortMembershipSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getInvalidCohortMembershipSource(env, consumerGroupId),
        TypeInformation.of(CohortMembership.class),
        KeyUtil.cohortMembershipKeySelector,
        validatedEventKafkaSegment.getCohortMembershipTopics().getInvalid().getSourceTopic());
  }

  public SingleOutputStreamOperator<View> getViewSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getViewSource(env, consumerGroupId),
        TypeInformation.of(View.class),
        KeyUtil.viewKeySelector,
        validatedEventKafkaSegment.getViewTopics().getValid().getSourceTopic());
  }

  public SingleOutputStreamOperator<View> getInvalidViewSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getInvalidViewSource(env, consumerGroupId),
        TypeInformation.of(View.class),
        KeyUtil.viewKeySelector,
        validatedEventKafkaSegment.getViewTopics().getInvalid().getSourceTopic());
  }

  public SingleOutputStreamOperator<DeliveryLog> getDeliveryLogSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getDeliveryLogSource(env, consumerGroupId),
        TypeInformation.of(DeliveryLog.class),
        KeyUtil.deliveryLogKeySelector,
        validatedEventKafkaSegment.getDeliveryLogTopics().getValid().getSourceTopic());
  }

  public SingleOutputStreamOperator<DeliveryLog> getInvalidDeliveryLogSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getInvalidDeliveryLogSource(env, consumerGroupId),
        TypeInformation.of(DeliveryLog.class),
        KeyUtil.deliveryLogKeySelector,
        validatedEventKafkaSegment.getDeliveryLogTopics().getInvalid().getSourceTopic());
  }

  public SingleOutputStreamOperator<Impression> getImpressionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getImpressionSource(env, consumerGroupId),
        TypeInformation.of(Impression.class),
        KeyUtil.impressionKeySelector,
        validatedEventKafkaSegment.getImpressionTopics().getValid().getSourceTopic());
  }

  public SingleOutputStreamOperator<Impression> getInvalidImpressionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getInvalidImpressionSource(env, consumerGroupId),
        TypeInformation.of(Impression.class),
        KeyUtil.impressionKeySelector,
        validatedEventKafkaSegment.getImpressionTopics().getInvalid().getSourceTopic());
  }

  public SingleOutputStreamOperator<Action> getActionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getActionSource(env, consumerGroupId),
        TypeInformation.of(Action.class),
        KeyUtil.actionKeySelector,
        validatedEventKafkaSegment.getActionTopics().getValid().getSourceTopic());
  }

  public SingleOutputStreamOperator<Action> getInvalidActionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return keepFirstSegment.keepFirst(
        directValidatedEventKafkaSource.getInvalidActionSource(env, consumerGroupId),
        TypeInformation.of(Action.class),
        KeyUtil.actionKeySelector,
        validatedEventKafkaSegment.getActionTopics().getInvalid().getSourceTopic());
  }

  // TODO - Diagnostics doesn't currently have a primary ID to deduplicate by.
  @Override
  public SingleOutputStreamOperator<Diagnostics> getDiagnosticsSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  @Override
  public SingleOutputStreamOperator<Diagnostics> getInvalidDiagnosticsSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }
}
