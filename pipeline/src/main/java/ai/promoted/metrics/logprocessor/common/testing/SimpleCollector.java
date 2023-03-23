package ai.promoted.metrics.logprocessor.common.testing;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;

public final class SimpleCollector<T> implements Collector<T> {
  private final List<T> records = new ArrayList();

  public List<T> getRecords() {
    return ImmutableList.copyOf(records);
  }

  @Override
  public void collect(T record) {
    records.add(record);
  }

  @Override
  public void close() {}
}
