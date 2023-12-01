package ai.promoted.metrics.logprocessor.common.job;

import static ai.promoted.metrics.logprocessor.common.job.KafkaSinkSegment.toKeyId;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.common.AnonUserRetainedUser;
import ai.promoted.proto.common.RetainedUser;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;

public class ValidatedEventKafkaSink implements ValidatedEventSink {
  private final KafkaSinkSegment sinkSegment;
  private final ValidatedEventKafkaSegment topicSegment;

  public ValidatedEventKafkaSink(
      KafkaSinkSegment sinkSegment, ValidatedEventKafkaSegment topicSegment) {
    this.sinkSegment = sinkSegment;
    this.topicSegment = topicSegment;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    // Do not register Segments passed into the constructor.
    return ImmutableSet.of();
  }

  @Override
  public void sinkLogUserUser(DataStream<LogUserUser> stream) {
    String topic = topicSegment.getLogUserUserTopics().getSinkTopic();
    sinkSegment.sinkTo(
        stream,
        topic,
        // For the key, logUserId is required for this table.
        sinkSegment.getAvroKafkaSink(
            topic, LogUserUser::getPlatformId, LogUserUser::getLogUserId, LogUserUser.class));
  }

  @Override
  public void sinkRetainedUser(DataStream<RetainedUser> stream) {
    String topic = topicSegment.getRetainedUserTopics().getSinkTopic();
    // For the key, userId is required on this table.  Otherwise it isn't written.
    sinkSegment.sinkTo(stream, topic, RetainedUser::getPlatformId, RetainedUser::getUserId);
  }

  @Override
  public void sinkAnonUserRetainedUser(DataStream<AnonUserRetainedUser> stream) {
    String topic = topicSegment.getAnonUserRetainedUserTopics().getSinkTopic();
    // For the key, anonUserId is required on this table.  Otherwise it isn't written.
    sinkSegment.sinkTo(
        stream, topic, AnonUserRetainedUser::getPlatformId, AnonUserRetainedUser::getAnonUserId);
  }

  @Override
  public void sinkValidationError(DataStream<ValidationError> stream) {
    String topic = topicSegment.getValidationErrorTopics().getSinkTopic();
    sinkSegment.sinkTo(
        stream,
        topic,
        sinkSegment.getAvroKafkaSink(
            topic,
            ValidationError::getPlatformId,
            error -> {
              // For the key:
              // - Prefer anonUserId.
              // - Then use whatever ID is available.
              // - If no ID, use the error type text.
              if (!error.getAnonUserId().isEmpty()) {
                return error.getAnonUserId();
              } else if (!error.getActionId().isEmpty()) {
                return error.getActionId();
              } else if (!error.getImpressionId().isEmpty()) {
                return error.getImpressionId();
              } else if (!error.getRequestId().isEmpty()) {
                return error.getRequestId();
              } else if (!error.getViewId().isEmpty()) {
                return error.getViewId();
              } else if (!error.getCohortMembershipId().isEmpty()) {
                return error.getCohortMembershipId();
              } else {
                return error.getErrorType().toString();
              }
            },
            ValidationError.class));
  }

  @Override
  public void sinkCohortMembership(DataStream<CohortMembership> stream) {
    String topic = topicSegment.getCohortMembershipTopics().getValid().getSinkTopic();
    sinkCohortMembership(stream, topic);
  }

  @Override
  public void sinkInvalidCohortMembership(DataStream<CohortMembership> stream) {
    String topic = topicSegment.getCohortMembershipTopics().getInvalid().getSinkTopic();
    sinkCohortMembership(stream, topic);
  }

  private void sinkCohortMembership(DataStream<CohortMembership> stream, String topic) {
    sinkSegment.sinkTo(
        stream,
        topic,
        CohortMembership::getPlatformId,
        toKeyId(CohortMembership::getUserInfo, CohortMembership::getMembershipId));
  }

  @Override
  public void sinkView(DataStream<View> stream) {
    String topic = topicSegment.getViewTopics().getValid().getSinkTopic();
    sinkView(stream, topic);
  }

  @Override
  public void sinkInvalidView(DataStream<View> stream) {
    String topic = topicSegment.getViewTopics().getInvalid().getSinkTopic();
    sinkView(stream, topic);
  }

  private void sinkView(DataStream<View> stream, String topic) {
    sinkSegment.sinkTo(
        stream, topic, View::getPlatformId, toKeyId(View::getUserInfo, View::getViewId));
  }

  @Override
  public void sinkDeliveryLog(DataStream<DeliveryLog> stream) {
    String topic = topicSegment.getDeliveryLogTopics().getValid().getSinkTopic();
    sinkDeliveryLog(stream, topic);
  }

  @Override
  public void sinkInvalidDeliveryLog(DataStream<DeliveryLog> stream) {
    String topic = topicSegment.getDeliveryLogTopics().getInvalid().getSinkTopic();
    sinkDeliveryLog(stream, topic);
  }

  private void sinkDeliveryLog(DataStream<DeliveryLog> stream, String topic) {
    sinkSegment.sinkTo(
        stream,
        topic,
        DeliveryLog::getPlatformId,
        toKeyId(
            deliveryLog -> deliveryLog.getRequest().getUserInfo(),
            deliveryLog -> deliveryLog.getRequest().getRequestId()));
  }

  @Override
  public void sinkImpression(DataStream<Impression> stream) {
    String topic = topicSegment.getImpressionTopics().getValid().getSinkTopic();
    sinkImpression(stream, topic);
  }

  @Override
  public void sinkInvalidImpression(DataStream<Impression> stream) {
    String topic = topicSegment.getImpressionTopics().getInvalid().getSinkTopic();
    sinkImpression(stream, topic);
  }

  private void sinkImpression(DataStream<Impression> stream, String topic) {
    sinkSegment.sinkTo(
        stream,
        topic,
        Impression::getPlatformId,
        toKeyId(Impression::getUserInfo, Impression::getImpressionId));
  }

  @Override
  public void sinkAction(DataStream<Action> stream) {
    String topic = topicSegment.getActionTopics().getValid().getSinkTopic();
    sinkAction(stream, topic);
  }

  @Override
  public void sinkInvalidAction(DataStream<Action> stream) {
    String topic = topicSegment.getActionTopics().getInvalid().getSinkTopic();
    sinkAction(stream, topic);
  }

  private void sinkAction(DataStream<Action> stream, String topic) {
    sinkSegment.sinkTo(
        stream, topic, Action::getPlatformId, toKeyId(Action::getUserInfo, Action::getActionId));
  }

  @Override
  public void sinkDiagnostics(DataStream<Diagnostics> stream) {
    String topic = topicSegment.getDiagnosticsTopics().getValid().getSinkTopic();
    sinkDiagnostics(stream, topic);
  }

  @Override
  public void sinkInvalidDiagnostics(DataStream<Diagnostics> stream) {
    String topic = topicSegment.getDiagnosticsTopics().getInvalid().getSinkTopic();
    sinkDiagnostics(stream, topic);
  }

  private void sinkDiagnostics(DataStream<Diagnostics> stream, String topic) {
    // TODO - add a primary key for Diagnostic records.
    sinkSegment.sinkTo(
        stream,
        topic,
        Diagnostics::getPlatformId,
        toKeyId(Diagnostics::getUserInfo, (diagnostics) -> ""));
  }
}
