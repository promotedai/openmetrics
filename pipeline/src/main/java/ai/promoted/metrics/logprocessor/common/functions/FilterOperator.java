package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializablePredicate;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * A simple filter Operator that splits the output stream to predicate matching (main output) and
 * non-matching (side output) streams.
 */
public class FilterOperator<T> extends ProcessFunction<T, T> {

  // True = main output of this function.  False = filterOutTag.
  private final SerializablePredicate<T> predicate;
  private final OutputTag<T> filteredOutTag;

  public FilterOperator(SerializablePredicate<T> predicate, OutputTag<T> filteredOutTag) {
    this.predicate = predicate;
    this.filteredOutTag = filteredOutTag;
  }

  public void processElement(T record, ProcessFunction<T, T>.Context ctx, Collector<T> out)
      throws Exception {
    if (predicate.test(record)) {
      out.collect(record);
    } else {
      ctx.output(filteredOutTag, record);
    }
  }

  public OutputTag<T> getFilteredOutTag() {
    return filteredOutTag;
  }
}
