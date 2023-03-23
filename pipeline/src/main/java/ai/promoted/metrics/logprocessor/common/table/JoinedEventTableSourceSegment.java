package ai.promoted.metrics.logprocessor.common.table;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.JoinedImpressionSegment;
import ai.promoted.proto.event.JoinedEvent;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import java.io.Serializable;
import java.util.List;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import picocli.CommandLine;

/** A segment for creating */
public class JoinedEventTableSourceSegment implements FlinkSegment {
  @CommandLine.Mixin public final JoinedImpressionSegment joinedImpressionSegment;
  private final BaseFlinkJob job;

  public JoinedEventTableSourceSegment(BaseFlinkJob job) {
    this.job = job;
    joinedImpressionSegment = new JoinedImpressionSegment(job);
  }

  @Override
  public void validateArgs() {
    joinedImpressionSegment.validateArgs();
  }

  @Override
  public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableList.<Class<? extends GeneratedMessageV3>>builder()
        .addAll(joinedImpressionSegment.getProtoClasses())
        .build();
  }

  public void createJoinedImpressionTable(
      StreamTableEnvironment tableEnv, DataStream<JoinedEvent> joinedEvents) {
    // TODO - try to move deduplication work to Flink SQL.  TBD because Flink SQL doesn't support
    // the same keep first semantics.
    DataStream<JoinedEvent> joinedImpressions =
        job.add(joinedEvents.filter(new IsJoinedImpression()), "filter-joined-impression");
    joinedImpressions = joinedImpressionSegment.getDeduplicatedJoinedImpression(joinedImpressions);
    DataStream<Row> rows =
        job.add(
            joinedImpressions.map(
                new ToJoinedImpressionRows(), JoinedImpressionTable.ROW_TYPE_INFORMATION),
            "to-joined-impression-row");
    Table table = tableEnv.fromDataStream(rows, JoinedImpressionTable.SCHEMA);
    tableEnv.createTemporaryView("joined_impression", table);
  }

  public static class IsJoinedImpression implements FilterFunction<JoinedEvent>, Serializable {
    @Override
    public boolean filter(JoinedEvent joinedEvent) throws Exception {
      return !joinedEvent.hasAction();
    }
  }

  private static class ToJoinedImpressionRows
      implements MapFunction<JoinedEvent, Row>, Serializable {
    @Override
    public Row map(JoinedEvent event) throws Exception {
      return JoinedImpressionTable.toRow(event);
    }
  }
}
