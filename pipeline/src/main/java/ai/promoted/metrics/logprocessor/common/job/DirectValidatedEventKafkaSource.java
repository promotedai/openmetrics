package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.proto.common.AnonUserRetainedUser;
import ai.promoted.proto.common.RetainedUser;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import picocli.CommandLine.Option;

/**
 * Provides direct (raw) access to ValidatedEventKafkaSource. Most consumers should use the
 * deduplicating version, ValidatedEventKafkaSource. We avoid the word "raw" or "duplicated" because
 * it's confusing.
 */
public class DirectValidatedEventKafkaSource implements FlinkSegment, ValidatedDataSourceProvider {
  public static final String DIRECT_VALIDATED_RO_DB_PREFIX = "direct_validated_ro";
  private final KafkaSourceSegment kafkaSourceSegment;
  private final ValidatedEventKafkaSegment validatedEventKafkaSegment;

  @Option(
      names = {"--overrideValidatedEventOutOfOrderPerTopic"},
      description = "The maxOutOfOrderness per topic. Default=no custom")
  public Map<String, Duration> overrideValidatedEventOutOfOrderPerTopic = new HashMap<>();

  @Option(
      names = {"--defaultValidatedEventOutOfOrder"},
      defaultValue = "PT1S",
      description = "The maxOutOfOrderness for validated events. Default=PT1S")
  public Duration defaultValidatedEventOutOfOrder = Duration.ofSeconds(1);

  public DirectValidatedEventKafkaSource(
      KafkaSourceSegment kafkaSourceSegment,
      ValidatedEventKafkaSegment validatedEventKafkaSegment) {
    this.kafkaSourceSegment = kafkaSourceSegment;
    this.validatedEventKafkaSegment = validatedEventKafkaSegment;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    // Do not register Segments passed into the constructor.
    return validatedEventKafkaSegment.getProtoClasses();
  }

  public SingleOutputStreamOperator<RetainedUser> getRetainedUserSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return kafkaSourceSegment.addProtobufSource(
        env,
        validatedEventKafkaSegment.getRetainedUserTopics().getSourceTopic(),
        consumerGroupId,
        RetainedUser.class,
        RetainedUser::parseFrom,
        overrideValidatedEventOutOfOrderPerTopic.getOrDefault(
            Constants.RETAINED_USER_EVENT_TOPIC_DATA_NAME, defaultValidatedEventOutOfOrder),
        RetainedUser::getCreateEventApiTimeMillis);
  }

  public SingleOutputStreamOperator<LogUserUser> getLogUserUserSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return kafkaSourceSegment.addAvroSource(
        env,
        validatedEventKafkaSegment.getLogUserUserTopics().getSourceTopic(),
        consumerGroupId,
        LogUserUser.class,
        overrideValidatedEventOutOfOrderPerTopic.getOrDefault(
            Constants.LOG_USER_USER_EVENT_TOPIC_DATA_NAME, defaultValidatedEventOutOfOrder),
        LogUserUser::getEventTimeMillis);
  }

  public SingleOutputStreamOperator<AnonUserRetainedUser> getAnonUserRetainedUserSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return kafkaSourceSegment.addProtobufSource(
        env,
        validatedEventKafkaSegment.getAnonUserRetainedUserTopics().getSourceTopic(),
        consumerGroupId,
        AnonUserRetainedUser.class,
        AnonUserRetainedUser::parseFrom,
        overrideValidatedEventOutOfOrderPerTopic.getOrDefault(
            Constants.ANON_USER_RETAINED_USER_EVENT_TOPIC_DATA_NAME,
            defaultValidatedEventOutOfOrder),
        AnonUserRetainedUser::getEventTimeMillis);
  }

  public SingleOutputStreamOperator<ValidationError> getValidationErrorSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return kafkaSourceSegment.addAvroSource(
        env,
        validatedEventKafkaSegment.getValidationErrorTopics().getSourceTopic(),
        consumerGroupId,
        ValidationError.class,
        overrideValidatedEventOutOfOrderPerTopic.getOrDefault(
            Constants.VALIDATION_ERROR_TOPIC_DATA_NAME, defaultValidatedEventOutOfOrder),
        error -> error.getTiming().getEventApiTimestamp());
  }

  public SingleOutputStreamOperator<CohortMembership> getCohortMembershipSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getCohortMembershipSource(
        env,
        validatedEventKafkaSegment.getCohortMembershipTopics().getValid().getSourceTopic(),
        consumerGroupId);
  }

  public SingleOutputStreamOperator<CohortMembership> getInvalidCohortMembershipSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getCohortMembershipSource(
        env,
        validatedEventKafkaSegment.getCohortMembershipTopics().getInvalid().getSourceTopic(),
        consumerGroupId);
  }

  private SingleOutputStreamOperator<CohortMembership> getCohortMembershipSource(
      StreamExecutionEnvironment env, String topic, String consumerGroupId) {
    return kafkaSourceSegment.addProtobufSource(
        env,
        topic,
        consumerGroupId,
        CohortMembership.class,
        CohortMembership::parseFrom,
        overrideValidatedEventOutOfOrderPerTopic.getOrDefault(
            Constants.COHORT_MEMBERSHIP_TOPIC_DATA_NAME, defaultValidatedEventOutOfOrder),
        cohortMembership -> cohortMembership.getTiming().getEventApiTimestamp());
  }

  public SingleOutputStreamOperator<View> getViewSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getViewSource(
        env,
        validatedEventKafkaSegment.getViewTopics().getValid().getSourceTopic(),
        consumerGroupId);
  }

  public SingleOutputStreamOperator<View> getInvalidViewSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getViewSource(
        env,
        validatedEventKafkaSegment.getViewTopics().getInvalid().getSourceTopic(),
        consumerGroupId);
  }

  private SingleOutputStreamOperator<View> getViewSource(
      StreamExecutionEnvironment env, String topic, String consumerGroupId) {
    return kafkaSourceSegment.addProtobufSource(
        env,
        topic,
        consumerGroupId,
        View.class,
        View::parseFrom,
        overrideValidatedEventOutOfOrderPerTopic.getOrDefault(
            Constants.VIEW_TOPIC_DATA_NAME, defaultValidatedEventOutOfOrder),
        view -> view.getTiming().getEventApiTimestamp());
  }

  public SingleOutputStreamOperator<DeliveryLog> getDeliveryLogSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getDeliveryLogSource(
        env,
        validatedEventKafkaSegment.getDeliveryLogTopics().getValid().getSourceTopic(),
        consumerGroupId);
  }

  public SingleOutputStreamOperator<DeliveryLog> getInvalidDeliveryLogSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getDeliveryLogSource(
        env,
        validatedEventKafkaSegment.getDeliveryLogTopics().getInvalid().getSourceTopic(),
        consumerGroupId);
  }

  private SingleOutputStreamOperator<DeliveryLog> getDeliveryLogSource(
      StreamExecutionEnvironment env, String topic, String consumerGroupId) {
    return kafkaSourceSegment.addProtobufSource(
        env,
        topic,
        consumerGroupId,
        DeliveryLog.class,
        DeliveryLog::parseFrom,
        overrideValidatedEventOutOfOrderPerTopic.getOrDefault(
            Constants.DELIVERY_LOG_TOPIC_DATA_NAME, defaultValidatedEventOutOfOrder),
        deliveryLog -> deliveryLog.getRequest().getTiming().getEventApiTimestamp());
  }

  public SingleOutputStreamOperator<Impression> getImpressionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getImpressionSource(
        env,
        validatedEventKafkaSegment.getImpressionTopics().getValid().getSourceTopic(),
        consumerGroupId);
  }

  public SingleOutputStreamOperator<Impression> getInvalidImpressionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getImpressionSource(
        env,
        validatedEventKafkaSegment.getImpressionTopics().getInvalid().getSourceTopic(),
        consumerGroupId);
  }

  private SingleOutputStreamOperator<Impression> getImpressionSource(
      StreamExecutionEnvironment env, String topic, String consumerGroupId) {
    return kafkaSourceSegment.addProtobufSource(
        env,
        topic,
        consumerGroupId,
        Impression.class,
        Impression::parseFrom,
        overrideValidatedEventOutOfOrderPerTopic.getOrDefault(
            Constants.IMPRESSION_TOPIC_DATA_NAME, defaultValidatedEventOutOfOrder),
        impression -> impression.getTiming().getEventApiTimestamp());
  }

  public SingleOutputStreamOperator<Action> getActionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getActionSource(
        env,
        validatedEventKafkaSegment.getActionTopics().getValid().getSourceTopic(),
        consumerGroupId);
  }

  public SingleOutputStreamOperator<Action> getInvalidActionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getActionSource(
        env,
        validatedEventKafkaSegment.getActionTopics().getInvalid().getSourceTopic(),
        consumerGroupId);
  }

  private SingleOutputStreamOperator<Action> getActionSource(
      StreamExecutionEnvironment env, String topic, String consumerGroupId) {
    return kafkaSourceSegment.addProtobufSource(
        env,
        topic,
        consumerGroupId,
        Action.class,
        Action::parseFrom,
        overrideValidatedEventOutOfOrderPerTopic.getOrDefault(
            Constants.ACTION_TOPIC_DATA_NAME, defaultValidatedEventOutOfOrder),
        action -> action.getTiming().getEventApiTimestamp());
  }

  public SingleOutputStreamOperator<Diagnostics> getDiagnosticsSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getDiagnosticsSource(
        env,
        validatedEventKafkaSegment.getDiagnosticsTopics().getValid().getSourceTopic(),
        consumerGroupId);
  }

  public SingleOutputStreamOperator<Diagnostics> getInvalidDiagnosticsSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return getDiagnosticsSource(
        env,
        validatedEventKafkaSegment.getDiagnosticsTopics().getInvalid().getSourceTopic(),
        consumerGroupId);
  }

  private SingleOutputStreamOperator<Diagnostics> getDiagnosticsSource(
      StreamExecutionEnvironment env, String topic, String consumerGroupId) {
    return kafkaSourceSegment.addProtobufSource(
        env,
        topic,
        consumerGroupId,
        Diagnostics.class,
        Diagnostics::parseFrom,
        overrideValidatedEventOutOfOrderPerTopic.getOrDefault(
            Constants.DIAGNOSTICS_TOPIC_DATA_NAME, defaultValidatedEventOutOfOrder),
        diagnostics -> diagnostics.getTiming().getEventApiTimestamp());
  }
}
