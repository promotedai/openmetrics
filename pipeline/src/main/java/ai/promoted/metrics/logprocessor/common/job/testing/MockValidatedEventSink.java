package ai.promoted.metrics.logprocessor.common.job.testing;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.metrics.logprocessor.common.job.S3FileOutput;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventSink;
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
import org.apache.flink.streaming.api.datastream.DataStream;

public class MockValidatedEventSink implements ValidatedEventSink {
  private final S3FileOutput s3FileOutput;

  public MockValidatedEventSink(S3FileOutput s3FileOutput) {
    this.s3FileOutput = s3FileOutput;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ValidatedEventKafkaSegment.PROTO_CLASSES;
  }

  @Override
  public void sinkLogUserUser(DataStream<LogUserUser> stream) {
    s3FileOutput.outputSpecificAvroRecordParquet(
        stream,
        LogUserUser.class,
        LogUserUser::getEventTimeMillis,
        s3FileOutput.getS3().getOutputDir("kafka", "metrics.default.log-user-user-event").build());
  }

  @Override
  public void sinkRetainedUser(DataStream<RetainedUser> stream) {
    s3FileOutput.sink(
        RetainedUser::getCreateEventApiTimeMillis,
        stream,
        RetainedUser.class,
        s3FileOutput.getS3().getOutputDir("kafka", "metrics.default.retained-user"));
  }

  @Override
  public void sinkAnonUserRetainedUser(DataStream<AnonUserRetainedUser> stream) {
    s3FileOutput.sink(
        AnonUserRetainedUser::getEventTimeMillis,
        stream,
        AnonUserRetainedUser.class,
        s3FileOutput
            .getS3()
            .getOutputDir("kafka", "metrics.default.anon-user-retained-user-event"));
  }

  @Override
  public void sinkValidationError(DataStream<ValidationError> stream) {
    s3FileOutput.outputSpecificAvroRecordParquet(
        stream,
        ValidationError.class,
        error -> error.getTiming().getEventApiTimestamp(),
        s3FileOutput.getS3().getOutputDir("kafka", "metrics.default.validation-error").build());
  }

  @Override
  public void sinkCohortMembership(DataStream<CohortMembership> stream) {
    sinkCohortMembership(stream, "metrics.default.cohort-membership");
  }

  @Override
  public void sinkInvalidCohortMembership(DataStream<CohortMembership> stream) {
    sinkCohortMembership(stream, "metrics.default.invalid-cohort-membership");
  }

  private void sinkCohortMembership(DataStream<CohortMembership> stream, String topic) {
    s3FileOutput.sink(
        stream,
        CohortMembership.class,
        CohortMembership::getTiming,
        s3FileOutput.getS3().getOutputDir("kafka", topic));
  }

  @Override
  public void sinkView(DataStream<View> stream) {
    sinkView(stream, "metrics.default.view");
  }

  @Override
  public void sinkInvalidView(DataStream<View> stream) {
    sinkView(stream, "metrics.default.invalid-view");
  }

  private void sinkView(DataStream<View> stream, String topic) {
    s3FileOutput.sink(
        stream, View.class, View::getTiming, s3FileOutput.getS3().getOutputDir("kafka", topic));
  }

  @Override
  public void sinkDeliveryLog(DataStream<DeliveryLog> stream) {
    sinkDeliveryLog(stream, "metrics.default.delivery-log");
  }

  @Override
  public void sinkInvalidDeliveryLog(DataStream<DeliveryLog> stream) {
    sinkDeliveryLog(stream, "metrics.default.invalid-delivery-log");
  }

  private void sinkDeliveryLog(DataStream<DeliveryLog> stream, String topic) {
    s3FileOutput.sink(
        stream,
        DeliveryLog.class,
        dl -> dl.getRequest().getTiming(),
        s3FileOutput.getS3().getOutputDir("kafka", topic));
  }

  @Override
  public void sinkImpression(DataStream<Impression> stream) {
    sinkImpression(stream, "metrics.default.impression");
  }

  @Override
  public void sinkInvalidImpression(DataStream<Impression> stream) {
    sinkImpression(stream, "metrics.default.invalid-impression");
  }

  private void sinkImpression(DataStream<Impression> stream, String topic) {
    s3FileOutput.sink(
        stream,
        Impression.class,
        Impression::getTiming,
        s3FileOutput.getS3().getOutputDir("kafka", topic));
  }

  @Override
  public void sinkAction(DataStream<Action> stream) {
    sinkAction(stream, "metrics.default.action");
  }

  @Override
  public void sinkInvalidAction(DataStream<Action> stream) {
    sinkAction(stream, "metrics.default.invalid-action");
  }

  private void sinkAction(DataStream<Action> stream, String topic) {
    s3FileOutput.sink(
        stream, Action.class, Action::getTiming, s3FileOutput.getS3().getOutputDir("kafka", topic));
  }

  @Override
  public void sinkDiagnostics(DataStream<Diagnostics> stream) {
    sinkDiagnostics(stream, "metrics.default.diagnostics");
  }

  @Override
  public void sinkInvalidDiagnostics(DataStream<Diagnostics> stream) {
    sinkDiagnostics(stream, "metrics.default.invalid-diagnostics");
  }

  private void sinkDiagnostics(DataStream<Diagnostics> stream, String topic) {
    s3FileOutput.sink(
        stream,
        Diagnostics.class,
        Diagnostics::getTiming,
        s3FileOutput.getS3().getOutputDir("kafka", topic));
  }
}
