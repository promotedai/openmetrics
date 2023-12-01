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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A {@link ProcessOperator} that supports changing the watermark a static delta. A positive delta
 * advances the watermark. A negative delta delays the watermark.
 */
public class ProcessOperatorWithWatermarkAddition<IN, OUT> extends ProcessOperator<IN, OUT> {
  private static final Logger LOGGER =
      LogManager.getLogger(ProcessOperatorWithWatermarkAddition.class);
  private static final long serialVersionUID = 1;

  private final Consumer<Watermark> emitter;
  private final boolean textLogWatermark;

  /**
   * Used to delay the watermark for a SingleOutputStreamOperator.
   *
   * @param watermarkDelay should be non-negative.
   */
  public static <T> SingleOutputStreamOperator<T> addWatermarkDelay(
      SingleOutputStreamOperator<T> input,
      TypeInformation<T> typeInformation,
      Duration watermarkDelay,
      String label,
      boolean textLogWatermarks) {
    Preconditions.checkArgument(
        !watermarkDelay.isNegative(), "watermarkDelay should not be negative");
    return addWatermarkDelta(
        input, typeInformation, watermarkDelay.negated(), label, textLogWatermarks);
  }

  /**
   * Used to advance the watermark for a SingleOutputStreamOperator.
   *
   * @param watermarkAdvance should be non-negative.
   */
  public static <T> SingleOutputStreamOperator<T> addWatermarkAdvance(
      SingleOutputStreamOperator<T> input,
      TypeInformation<T> typeInformation,
      Duration watermarkAdvance,
      String label,
      boolean textLogWatermarks) {
    Preconditions.checkArgument(
        !watermarkAdvance.isNegative(), "watermarkAdvance should not be negative");
    return addWatermarkDelta(input, typeInformation, watermarkAdvance, label, textLogWatermarks);
  }

  private static <T> SingleOutputStreamOperator<T> addWatermarkDelta(
      SingleOutputStreamOperator<T> input,
      TypeInformation<T> typeInformation,
      Duration watermarkDelta,
      String label,
      boolean textLogWatermarks) {
    if (watermarkDelta.isZero()) {
      return input;
    }

    // Delays the watermark.
    ProcessFunction<T, T> noop =
        new ProcessFunction<>() {
          @Override
          public void processElement(
              T record, ProcessFunction<T, T>.Context ctx, Collector<T> collector)
              throws Exception {
            collector.collect(record);
          }
        };
    return input.transform(
        label,
        typeInformation,
        new ProcessOperatorWithWatermarkAddition<T, T>(
            noop, watermarkDelta.toMillis(), textLogWatermarks));
  }

  ProcessOperatorWithWatermarkAddition(
      ProcessFunction<IN, OUT> flatMapper, long watermarkDelta, boolean textLogWatermark) {
    super(flatMapper);
    this.textLogWatermark = textLogWatermark;
    if (watermarkDelta == 0) {
      // emits watermark without delay
      emitter = (Consumer<Watermark> & Serializable) (Watermark mark) -> output.emitWatermark(mark);
    } else {
      // emits watermark with delay
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
