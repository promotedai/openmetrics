package ai.promoted.metrics.logprocessor.common.job;

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
import org.apache.flink.streaming.api.datastream.DataStream;

/** An interface for sinking validated events. */
public interface ValidatedEventSink extends FlinkSegment {

  // TODO - redesign to sinkAnonUserRetainedUser.
  void sinkLogUserUser(DataStream<LogUserUser> stream);

  void sinkRetainedUser(DataStream<RetainedUser> stream);

  void sinkAnonUserRetainedUser(DataStream<AnonUserRetainedUser> stream);

  void sinkValidationError(DataStream<ValidationError> stream);

  void sinkCohortMembership(DataStream<CohortMembership> stream);

  void sinkInvalidCohortMembership(DataStream<CohortMembership> stream);

  void sinkView(DataStream<View> stream);

  void sinkInvalidView(DataStream<View> stream);

  void sinkDeliveryLog(DataStream<DeliveryLog> stream);

  void sinkInvalidDeliveryLog(DataStream<DeliveryLog> stream);

  void sinkImpression(DataStream<Impression> stream);

  void sinkInvalidImpression(DataStream<Impression> stream);

  void sinkAction(DataStream<Action> stream);

  void sinkInvalidAction(DataStream<Action> stream);

  void sinkDiagnostics(DataStream<Diagnostics> stream);

  void sinkInvalidDiagnostics(DataStream<Diagnostics> stream);
}
