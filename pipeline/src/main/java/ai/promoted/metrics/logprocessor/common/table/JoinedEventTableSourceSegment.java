package ai.promoted.metrics.logprocessor.common.table;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.CompositeFlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.JoinedImpression;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.Set;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/** A segment for creating */
public class JoinedEventTableSourceSegment implements CompositeFlinkSegment {
  private final BaseFlinkJob job;

  public JoinedEventTableSourceSegment(BaseFlinkJob job) {
    this.job = job;
  }

  @Override
  public Set<FlinkSegment> getInnerFlinkSegments() {
    return ImmutableSet.of();
  }

  public void createJoinedImpressionTable(
      StreamTableEnvironment tableEnv, DataStream<JoinedImpression> joinedImpressions) {
    // TODO - try to move deduplication work to Flink SQL.  TBD because Flink SQL doesn't support
    // the same keep first semantics.
    DataStream<Row> rows =
        job.add(
            joinedImpressions.map(
                new ToJoinedImpressionRows(), JoinedImpressionTable.ROW_TYPE_INFORMATION),
            "to-joined-impression-row");
    Table table = tableEnv.fromDataStream(rows, JoinedImpressionTable.SCHEMA);
    tableEnv.createTemporaryView("joined_impression", table);
  }

  public void createAttributedActionTable(
      StreamTableEnvironment tableEnv, DataStream<AttributedAction> attributedActions) {
    // TODO - try to move deduplication work to Flink SQL.  TBD because Flink SQL doesn't support
    // the same keep first semantics.
    DataStream<Row> rows =
        job.add(
            attributedActions.map(
                new ToAttributedActionRows(), AttributedActionTable.ROW_TYPE_INFORMATION),
            "to-attributed-action-row");
    Table table = tableEnv.fromDataStream(rows, AttributedActionTable.SCHEMA);
    tableEnv.createTemporaryView("attributed_action", table);
  }

  private static class ToJoinedImpressionRows
      implements MapFunction<JoinedImpression, Row>, Serializable {
    @Override
    public Row map(JoinedImpression impression) throws Exception {
      return JoinedImpressionTable.toRow(impression);
    }
  }

  private static class ToAttributedActionRows
      implements MapFunction<AttributedAction, Row>, Serializable {
    @Override
    public Row map(AttributedAction action) throws Exception {
      return AttributedActionTable.toRow(action);
    }
  }
}
