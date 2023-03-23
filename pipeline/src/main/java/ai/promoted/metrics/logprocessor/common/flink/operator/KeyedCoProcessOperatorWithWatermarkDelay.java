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
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** A {@link KeyedCoProcessOperator} that supports holding back watermarks with a static delay. */
public class KeyedCoProcessOperatorWithWatermarkDelay<K, IN1, IN2, OUT>
    extends KeyedCoProcessOperator<K, IN1, IN2, OUT> {
  private static final Logger LOGGER =
      LogManager.getLogger(KeyedCoProcessOperatorWithWatermarkDelay.class);
  private static final long serialVersionUID = -7435774708099223442L;

  private final Consumer<Watermark> emitter;
  private final boolean textLogWatermark;

  public KeyedCoProcessOperatorWithWatermarkDelay(
      KeyedCoProcessFunction<K, IN1, IN2, OUT> flatMapper,
      long watermarkDelay,
      boolean textLogWatermark) {
    super(flatMapper);
    this.textLogWatermark = textLogWatermark;
    Preconditions.checkArgument(watermarkDelay >= 0, "The watermark delay should be non-negative.");
    if (watermarkDelay == 0) {
      // emits watermark without delay
      emitter = (Consumer<Watermark> & Serializable) (Watermark mark) -> output.emitWatermark(mark);
    } else {
      // emits watermark with delay
      emitter =
          (Consumer<Watermark> & Serializable)
              (Watermark mark) ->
                  output.emitWatermark(new Watermark(mark.getTimestamp() - watermarkDelay));
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
