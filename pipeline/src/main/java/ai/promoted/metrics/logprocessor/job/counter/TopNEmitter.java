package ai.promoted.metrics.logprocessor.job.counter;

import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Duration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Flink flat map function that tracks the top n number of elements by frequency.
 *
 * <p>Generally, this output should be used as the basis for a broadcast stream. We need to keep the
 * data volume as small as possible, so the output is really an update stream for the state.
 */
public class TopNEmitter<T> extends RichFlatMapFunction<T, Tuple2<T, Boolean>> {
  private static final Logger LOGGER = LogManager.getLogger(TopNEmitter.class);

  // The number (n) of items to allow.
  private final long n;
  // Type information for T
  private final TypeInformation<T> typeInfo;
  // The broadcast state map: {T -> keep}
  final MapStateDescriptor<T, Boolean> broadcastDescriptor;
  // The time delay to wait to output in order to model top queries better.
  // TODO: make this an argument
  private final Duration initialDelay = Duration.ofDays(1);

  private static final class HeapInfo<T> {
    long count;
    // Will be >=0 if this particular element is part of the top n/in the heap.
    long index = -1;
  }

  private MapState<T, HeapInfo<T>> infos;
  // Treat this map as a list.
  private MapState<Long, T> minHeap;
  private ValueState<Long> heapSize;

  public TopNEmitter(long n, TypeInformation<T> typeInfo) {
    this.n = n;
    this.typeInfo = typeInfo;
    this.broadcastDescriptor = new MapStateDescriptor<>("top-n-broadcast", typeInfo, Types.BOOLEAN);
  }

  @Override
  public void open(Configuration config) {
    infos =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "infos", typeInfo, TypeInformation.of(new TypeHint<HeapInfo<T>>() {})));
    minHeap =
        getRuntimeContext().getMapState(new MapStateDescriptor<>("min-heap", Types.LONG, typeInfo));
    heapSize = getRuntimeContext().getState(new ValueStateDescriptor<>("heap-size", Types.LONG));
  }

  @Override
  public void flatMap(T in, Collector<Tuple2<T, Boolean>> out) throws Exception {
    HeapInfo<T> info = infos.contains(in) ? infos.get(in) : new HeapInfo<>();
    info.count++;

    if (info.index >= 0) {
      // Already in minHeap, update invariants.
      infos.put(in, info);
      siftDown(info.index);
    } else {
      // Try adding it to the minHeap.
      long size = heapSize.value() != null ? heapSize.value() : 0;
      if (size > n) shrink(out);

      T root = minHeap.get(0L);
      if (size < n) {
        out.collect(Tuple2.of(in, true));
        infos.put(in, info);
        minHeap.put(size, in);
        heapSize.update(size + 1);
        siftUp(size);
      } else if (info.count > getCount(root)) {
        out.collect(Tuple2.of(root, false));
        HeapInfo<T> oldRootInfo = infos.get(root);
        // "Evict" the root value from the top n.
        oldRootInfo.index = -1;
        infos.put(root, oldRootInfo);

        // Replace the root with the new value and sift.
        out.collect(Tuple2.of(in, true));
        minHeap.put(0L, in);
        info.index = 0;
        infos.put(in, info);
        siftDown(0L);
      } // else, do nothing as it's not part of the top n minHeap.
    }
  }

  private void shrink(Collector<Tuple2<T, Boolean>> out) throws Exception {
    long size = heapSize.value() != null ? heapSize.value() : 0;
    LOGGER.warn("Shrinking min heap from {} to {} elements as N has shrunk", size, n);

    while (size > n) {
      // "Evict" the root value from the top n.
      T root = minHeap.get(0L);
      out.collect(Tuple2.of(root, false));
      HeapInfo<T> oldRootInfo = infos.get(root);
      // "Evict" the root value from the top n.
      oldRootInfo.index = -1;
      infos.put(root, oldRootInfo);

      // Swap leftmost leaf with root and sift down.
      minHeap.put(0L, minHeap.get(size--));
      heapSize.update(size);
      siftDown(0);
    }
  }

  private void siftUp(long i) throws Exception {
    while (i != 0 && getCount(i) < getCount(parent(i))) {
      swap(parent(i), i);
      i = parent(i);
    }
  }

  private void siftDown(long i) throws Exception {
    long smallest = left(i);
    if (smallest >= heapSize.value()) return;
    long r = right(i);
    if (r < heapSize.value() && getCount(r) < getCount(smallest)) smallest = r;
    if (getCount(smallest) < getCount(i)) {
      swap(i, smallest);
      siftDown(smallest);
    }
  }

  private void swap(long parent, long child) throws Exception {
    setIndex(parent, minHeap.get(child));
    setIndex(child, minHeap.get(parent));
  }

  private void setIndex(long i, T in) throws Exception {
    minHeap.put(i, in);
    HeapInfo<T> info = infos.get(in);
    info.index = i;
    infos.put(in, info);
  }

  private long getCount(T node) throws Exception {
    return infos.get(checkNotNull(node)).count;
  }

  private long getCount(long i) throws Exception {
    return getCount(minHeap.get(i));
  }

  private static long parent(long index) {
    return (index - 1) / 2;
  }

  private static long left(long index) {
    return 2 * index + 1;
  }

  private static long right(long index) {
    return 2 * index + 2;
  }
}
