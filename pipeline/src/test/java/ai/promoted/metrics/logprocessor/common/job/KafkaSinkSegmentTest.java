package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.proto.event.Action;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class KafkaSinkSegmentTest {
  @ParameterizedTest
  @ValueSource(ints = {1, 10})
  public void testGenSortNodeBeforeKafkaSink(int sinkParallelism) {
    KafkaSinkSegment kafkaSinkSegment = getKafkaSinkSegment(sinkParallelism);
    kafkaSinkSegment.sortBeforeSink = true;
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Action> stream = env.fromCollection(List.of(Action.newBuilder().build()));
    kafkaSinkSegment.sinkTo(stream, "topic1", (a) -> 1L, (a) -> "");
    StreamGraph graph = env.getStreamGraph(false);
    Optional<StreamNode> sortNode =
        graph.getStreamNodes().stream()
            .filter(n -> n.getOperatorName().equals("sort-sink-kafka-topic1"))
            .findFirst();
    Assertions.assertTrue(sortNode.isPresent());
    Assertions.assertTrue(
        sortNode.get().getOutEdges().get(0).getPartitioner() instanceof ForwardPartitioner);
    Assertions.assertEquals(sinkParallelism, sortNode.get().getParallelism());
  }

  private static KafkaSinkSegment getKafkaSinkSegment(int sinkParallelism) {
    BaseFlinkJob flinkJob =
        new BaseFlinkJob() {
          @Override
          protected void startJob() {}

          @Override
          protected String getDefaultBaseJobName() {
            return "";
          }

          @Override
          public Set<FlinkSegment> getInnerFlinkSegments() {
            return Collections.emptySet();
          }
        };
    flinkJob.maxParallelism = Integer.MAX_VALUE;
    flinkJob.defaultSinkParallelism = sinkParallelism;
    KafkaSegment kafkaSegment = new KafkaSegment();
    return new KafkaSinkSegment(flinkJob, kafkaSegment);
  }
}
