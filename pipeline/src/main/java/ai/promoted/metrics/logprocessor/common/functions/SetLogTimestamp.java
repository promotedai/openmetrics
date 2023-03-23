package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.proto.common.Timing;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class SetLogTimestamp<T, B> extends ProcessFunction<T, T> {
  private final SerializableFunction<T, B> toBuilder;
  private final SerializableFunction<B, Timing.Builder> getTimingBuilder;
  private final SerializableFunction<B, T> build;

  public SetLogTimestamp(
      SerializableFunction<T, B> toBuilder,
      SerializableFunction<B, Timing.Builder> getTimingBuilder,
      SerializableFunction<B, T> build) {
    this.toBuilder = toBuilder;
    this.getTimingBuilder = getTimingBuilder;
    this.build = build;
  }

  public static final SetLogTimestamp<User, User.Builder> forUser =
      new SetLogTimestamp<>(User::toBuilder, User.Builder::getTimingBuilder, User.Builder::build);

  public static final SetLogTimestamp<View, View.Builder> forView =
      new SetLogTimestamp<>(View::toBuilder, View.Builder::getTimingBuilder, View.Builder::build);

  public static final SetLogTimestamp<AutoView, AutoView.Builder> forAutoView =
      new SetLogTimestamp<>(
          AutoView::toBuilder, AutoView.Builder::getTimingBuilder, AutoView.Builder::build);

  public static final SetLogTimestamp<DeliveryLog, DeliveryLog.Builder> forDeliveryLog =
      new SetLogTimestamp<>(
          DeliveryLog::toBuilder,
          builder -> builder.getRequestBuilder().getTimingBuilder(),
          DeliveryLog.Builder::build);

  public static final SetLogTimestamp<Request, Request.Builder> forRequest =
      new SetLogTimestamp<>(
          Request::toBuilder, Request.Builder::getTimingBuilder, Request.Builder::build);

  public static final SetLogTimestamp<Impression, Impression.Builder> forImpression =
      new SetLogTimestamp<>(
          Impression::toBuilder, Impression.Builder::getTimingBuilder, Impression.Builder::build);

  public static final SetLogTimestamp<Action, Action.Builder> forAction =
      new SetLogTimestamp<>(
          Action::toBuilder, Action.Builder::getTimingBuilder, Action.Builder::build);

  @Override
  public void processElement(T row, Context context, Collector<T> collector) throws Exception {
    B builder = toBuilder.apply(row);
    getTimingBuilder.apply(builder).setLogTimestamp(context.timestamp());
    collector.collect(build.apply(builder));
  }
}
