package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.common.RetainedUser;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;
import javax.annotation.Nullable;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The provider for all validated data sources. Usually, we should avoid using default keyword for
 * explicit overwriting in child classes.
 */
public interface ValidatedDataSourceProvider {
  default SingleOutputStreamOperator<RetainedUser> getRetainedUserSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<RetainedUser> getRetainedUserSource(
      StreamExecutionEnvironment env,
      String consumerGroupId,
      @Nullable KeySelector<RetainedUser, ?> keySelector) {
    return null;
  }

  default SingleOutputStreamOperator<LogUserUser> getLogUserUserSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<LogUserUser> getLogUserUserSource(
      StreamExecutionEnvironment env,
      String consumerGroupId,
      KeySelector<LogUserUser, ?> keySelector) {
    return null;
  }

  default SingleOutputStreamOperator<ValidationError> getValidationErrorSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<ValidationError> getValidationErrorSource(
      StreamExecutionEnvironment env,
      String consumerGroupId,
      @Nullable KeySelector<ValidationError, ?> keySelector) {
    return null;
  }

  default SingleOutputStreamOperator<CohortMembership> getCohortMembershipSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<CohortMembership> getInvalidCohortMembershipSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<View> getViewSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<View> getInvalidViewSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<DeliveryLog> getDeliveryLogSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<DeliveryLog> getInvalidDeliveryLogSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<Impression> getImpressionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<Impression> getInvalidImpressionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<Action> getActionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<Action> getInvalidActionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<Diagnostics> getDiagnosticsSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }

  default SingleOutputStreamOperator<Diagnostics> getInvalidDiagnosticsSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return null;
  }
}
