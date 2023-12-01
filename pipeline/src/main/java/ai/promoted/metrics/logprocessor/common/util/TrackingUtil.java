package ai.promoted.metrics.logprocessor.common.util;

import ai.promoted.metrics.error.ValidationError;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableSupplier;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;
import com.google.common.annotations.VisibleForTesting;

public class TrackingUtil {

  //  public static <T> SingleOutputStreamOperator<T> mapWithFunction(
  //      SingleOutputStreamOperator<T> inputStream, SerializableFunction<T, T> setTimeFunction) {
  //    return inputStream.map((MapFunction<T, T>) setTimeFunction::apply);
  //  }

  @VisibleForTesting
  public static SerializableSupplier<Long> processingTimeSupplier = System::currentTimeMillis;

  public static long getProcessingTime() {
    return processingTimeSupplier.get();
  }

  public static final SerializableFunction<ValidationError, ValidationError>
      VALIDATION_ERROR_PROCESSING_TIME_SETTER =
          validationError -> {
            validationError.getTiming().setProcessingTimestamp(getProcessingTime());
            return validationError;
          };
  public static final SerializableFunction<CohortMembership, CohortMembership>
      COHORT_MEMBERSHIP_PROCESSING_TIME_SETTER =
          cohortMembership -> {
            CohortMembership.Builder builder = cohortMembership.toBuilder();
            builder.getTimingBuilder().setProcessingTimestamp(getProcessingTime());
            return builder.build();
          };

  public static final SerializableFunction<View, View> VIEW_PROCESSING_TIME_SETTER =
      view -> {
        View.Builder builder = view.toBuilder();
        builder.getTimingBuilder().setProcessingTimestamp(getProcessingTime());
        return builder.build();
      };

  public static final SerializableFunction<DeliveryLog, DeliveryLog>
      DELIVERY_LOG_PROCESSING_TIME_SETTER =
          deliveryLog -> {
            DeliveryLog.Builder builder = deliveryLog.toBuilder();
            builder
                .getRequestBuilder()
                .getTimingBuilder()
                .setProcessingTimestamp(getProcessingTime());
            return builder.build();
          };
  public static final SerializableFunction<Impression, Impression>
      IMPRESSION_PROCESSING_TIME_SETTER =
          impression -> {
            Impression.Builder builder = impression.toBuilder();
            builder.getTimingBuilder().setProcessingTimestamp(getProcessingTime());
            return builder.build();
          };

  public static final SerializableFunction<Action, Action> ACTION_PROCESSING_TIME_SETTER =
      action -> {
        Action.Builder builder = action.toBuilder();
        builder.getTimingBuilder().setProcessingTimestamp(getProcessingTime());
        return builder.build();
      };
  public static final SerializableFunction<Diagnostics, Diagnostics>
      DIAGNOSTICS_PROCESSING_TIME_SETTER =
          diagnostics -> {
            Diagnostics.Builder builder = diagnostics.toBuilder();
            builder.getTimingBuilder().setProcessingTimestamp(getProcessingTime());
            return builder.build();
          };
}
