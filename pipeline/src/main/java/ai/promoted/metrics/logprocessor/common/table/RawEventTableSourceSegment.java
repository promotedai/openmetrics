package ai.promoted.metrics.logprocessor.common.table;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.MetricsApiKafkaSource.SplitSources;
import ai.promoted.metrics.logprocessor.common.job.RawImpressionSegment;
import ai.promoted.metrics.logprocessor.common.job.RawViewSegment;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import java.io.Serializable;
import java.util.List;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import picocli.CommandLine;

/** A Segment for interacting with raw events as Flink Tables. */
public class RawEventTableSourceSegment implements FlinkSegment {
  @CommandLine.Mixin public final RawViewSegment rawViewSegment;
  @CommandLine.Mixin public final RawImpressionSegment rawImpressionSegment;

  private final BaseFlinkJob job;

  public RawEventTableSourceSegment(BaseFlinkJob job) {
    this.job = job;
    rawViewSegment = new RawViewSegment(job);
    rawImpressionSegment = new RawImpressionSegment(job);
  }

  @Override
  public void validateArgs() {
    rawViewSegment.validateArgs();
    rawImpressionSegment.validateArgs();
  }

  @Override
  public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableList.<Class<? extends GeneratedMessageV3>>builder()
        .addAll(rawViewSegment.getProtoClasses())
        .addAll(rawImpressionSegment.getProtoClasses())
        .build();
  }

  public void createViewTable(StreamTableEnvironment tableEnv, SplitSources splitLogRequest) {
    // TODO - try to move deduplication work to Flink SQL.  TBD because Flink SQL doesn't support
    // the same keep first semantics.
    DataStream<View> protoStream =
        rawViewSegment.getDeduplicatedView(splitLogRequest.getRawViewSource());
    DataStream<Row> rows =
        job.add(protoStream.map(ViewTable::toRow, ViewTable.ROW_TYPE_INFORMATION), "to-view-row");
    Table table = tableEnv.fromDataStream(rows, ViewTable.SCHEMA);
    tableEnv.createTemporaryView("view", table);
  }

  public void createImpressionTable(StreamTableEnvironment tableEnv, SplitSources splitLogRequest) {
    // TODO - try to move deduplication work to Flink SQL.  TBD because Flink SQL doesn't support
    // the same keep first semantics.
    DataStream<Impression> impressions =
        rawImpressionSegment.getDeduplicatedImpression(splitLogRequest.getRawImpressionSource());
    DataStream<Row> impressionRows =
        job.add(
            impressions.map(ImpressionTable::toImpressionRow, ImpressionTable.IMPRESSION_ROW),
            "to-impression-row");
    Table table = tableEnv.fromDataStream(impressionRows, ImpressionTable.IMPRESSION_SCHEMA);
    tableEnv.createTemporaryView("impression", table);
  }

  public void createActionTable(StreamTableEnvironment tableEnv, DataStream<Action> actions) {
    DataStream<Row> actionRows =
        job.add(actions.map(ActionTable::toActionRow, ActionTable.ACTION_ROW), "to-action-row");
    Table table = tableEnv.fromDataStream(actionRows, ActionTable.ACTION_SCHEMA);
    tableEnv.createTemporaryView("action", table);
  }

  public void createActionCartContentTable(
      StreamTableEnvironment tableEnv, DataStream<Action> actions) {
    DataStream<Row> actionCartContentRows =
        job.add(
            actions.flatMap(new ToFlatActionCartContentRows(), ActionTable.ACTION_CART_CONTENT_ROW),
            "to-action-cart-content-row");
    Table table =
        tableEnv.fromDataStream(actionCartContentRows, ActionTable.ACTION_CART_CONTENT_SCHEMA);
    tableEnv.createTemporaryView("action_cart_content", table);
  }

  private static class ToFlatActionCartContentRows
      implements FlatMapFunction<Action, Row>, Serializable {
    @Override
    public void flatMap(Action action, Collector<Row> out) throws Exception {
      ActionTable.toActionCartContentRow(action).forEach(out::collect);
    }
  }
}
