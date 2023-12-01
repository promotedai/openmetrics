/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Copied from Flink's org.apache.flink.table.runtime.operators.join;

package ai.promoted.metrics.logprocessor.common.flink.operator;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A {@link KeyedProcessOperator} that supports changing the watermark a static delta. A positive
 * delta advances the watermark. A negative delta delays the watermark.
 */
public class KeyedProcessOperatorWithWatermarkAddition<K, IN, OUT>
    extends KeyedProcessOperator<K, IN, OUT> {
  private static final Logger LOGGER =
      LogManager.getLogger(KeyedProcessOperatorWithWatermarkAddition.class);
  private static final long serialVersionUID = 1;

  private final Consumer<Watermark> emitter;
  private final boolean textLogWatermark;

  /**
   * Used to delay the watermark for the KeyedProcessFunction.
   *
   * @param watermarkDelay should be non-negative.
   */
  public static <K, IN, OUT> KeyedProcessOperatorWithWatermarkAddition<K, IN, OUT> withDelay(
      KeyedProcessFunction<K, IN, OUT> flatMapper,
      Duration watermarkDelay,
      boolean textLogWatermark) {
    Preconditions.checkArgument(
        !watermarkDelay.isNegative(), "watermarkDelay should not be negative");
    return new KeyedProcessOperatorWithWatermarkAddition<K, IN, OUT>(
        flatMapper, watermarkDelay.negated().toMillis(), textLogWatermark);
  }

  KeyedProcessOperatorWithWatermarkAddition(
      KeyedProcessFunction<K, IN, OUT> flatMapper, long watermarkDelta, boolean textLogWatermark) {
    super(flatMapper);
    this.textLogWatermark = textLogWatermark;
    if (watermarkDelta == 0) {
      // emits original watermark
      emitter = (Consumer<Watermark> & Serializable) (Watermark mark) -> output.emitWatermark(mark);
    } else {
      // emits watermark+delta
      emitter =
          (Consumer<Watermark> & Serializable)
              (Watermark mark) ->
                  output.emitWatermark(new Watermark(mark.getTimestamp() + watermarkDelta));
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    Optional<InternalTimeServiceManager<?>> timeServiceManager = getTimeServiceManager();
    if (timeServiceManager.isPresent()) {
      timeServiceManager.get().advanceWatermark(mark);
    }
    emitter.accept(mark);

    if (textLogWatermark) {
      LOGGER.info(
          "processWatermark {} - {}",
          this.getUserFunction().getClass().getSimpleName(),
          mark.getTimestamp());
    }
  }
}
