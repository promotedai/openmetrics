package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializablePredicate;
import java.io.IOException;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * KeyedCoProcessFunction that only emits the "left" stream iff it has seen some part of it (using
 * the rightExtractor function) on the "right" stream.
 *
 * <p>This function skips nulls on both the left and right streams.
 */
public class RightSeenOutput<LEFT, RIGHT> extends KeyedCoProcessFunction<RIGHT, LEFT, RIGHT, LEFT> {
  private static final Logger LOGGER = LogManager.getLogger(RightSeenOutput.class);

  private final TypeInformation<LEFT> leftType;
  private final TypeInformation<RIGHT> rightType;
  private final SerializableFunction<LEFT, RIGHT> rightExtractor;
  private final SerializablePredicate<LEFT> emitImmediately;

  private ValueState<Boolean> seen;
  private ListState<LEFT> delayed;

  /**
   * Constructor.
   *
   * @param leftType TypeInformation of the left stream
   * @param rightType TypeInformation of the right stream
   * @param rightExtractor Function to produce a rightType element from a left element
   */
  public RightSeenOutput(
      TypeInformation<LEFT> leftType,
      TypeInformation<RIGHT> rightType,
      SerializableFunction<LEFT, RIGHT> rightExtractor,
      SerializablePredicate<LEFT> emitImmediately) {
    this.leftType = leftType;
    this.rightType = rightType;
    this.rightExtractor = rightExtractor;
    this.emitImmediately = emitImmediately;
  }

  @Override
  public void open(Configuration config) {
    seen = getRuntimeContext().getState(new ValueStateDescriptor<>("seen", Types.BOOLEAN));
    delayed = getRuntimeContext().getListState(new ListStateDescriptor<>("delayed", leftType));
  }

  @Override
  public void processElement1(LEFT left, Context ctx, Collector<LEFT> out) throws Exception {
    LOGGER.trace(
        "{} {} {} processElement1: {}",
        ctx.timestamp(),
        ctx.timerService().currentWatermark(),
        ctx.getCurrentKey(),
        left);
    if (left == null) return;
    if (emitImmediately.test(left)) {
      out.collect(left);
      return;
    }
    delayed.add(left);
    // We output on timer to coordinate against the watermark.  The delay can therefore be 0.
    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
  }

  @Override
  public void onTimer(long ts, OnTimerContext ctx, Collector<LEFT> out) throws Exception {
    LOGGER.trace(
        "{} {} {} onTimer", ts, ctx.timerService().currentWatermark(), ctx.getCurrentKey());
    if (Boolean.TRUE.equals(seen.value())) {
      for (LEFT left : delayed.get()) {
        LOGGER.trace(
            "{} {} contained: {} output: {}",
            ctx.timestamp(),
            ctx.timerService().currentWatermark(),
            ctx.getCurrentKey(),
            left);
        out.collect(left);
      }
    }
    delayed.clear();
  }

  @Override
  public void processElement2(RIGHT right, Context ctx, Collector<LEFT> out) throws IOException {
    LOGGER.trace(
        "{} {} {} processElement2: {}",
        ctx.timestamp(),
        ctx.timerService().currentWatermark(),
        ctx.getCurrentKey(),
        right);
    if (right == null) return;
    if (!Boolean.TRUE.equals(seen.value())) {
      LOGGER.trace("{} {} seen: {}", ctx.timestamp(), ctx.timerService().currentWatermark(), right);
      seen.update(true);
    }
  }
}
