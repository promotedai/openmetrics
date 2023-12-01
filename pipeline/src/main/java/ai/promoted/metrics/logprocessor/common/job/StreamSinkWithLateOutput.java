package ai.promoted.metrics.logprocessor.common.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

public class StreamSinkWithLateOutput<T> {

  public StreamSinkWithLateOutput(DataStreamSink<?> sink, DataStream<T> lateEventStream) {
    this.sink = sink;
    this.lateEventStream = lateEventStream;
  }

  private final DataStreamSink<?> sink;

  private final DataStream<T> lateEventStream;

  public DataStreamSink<?> getSink() {
    return sink;
  }

  public DataStream<T> getLateEventStream() {
    return lateEventStream;
  }
}
