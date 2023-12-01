package ai.promoted.metrics.logprocessor.common.table;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.common.Struct1;
import ai.promoted.proto.common.Struct2;
import ai.promoted.proto.common.Value1;
import ai.promoted.proto.common.Value2;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

public class FlinkTableUtilsMiniclusterTest {
  @Test
  public void testProtoTableDataStreamConversion() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.registerTypeWithKryoSerializer(Struct1.class, ProtobufSerializer.class);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    Struct1 struct1 =
        Struct1.newBuilder()
            .putAllFields(
                Map.of(
                    "key1",
                    Value1.newBuilder()
                        .setStructValue(
                            Struct2.newBuilder()
                                .putFields(
                                    "key2", Value2.newBuilder().setStringValue("value2").build())
                                .build())
                        .build(),
                    "ts",
                    Value1.newBuilder().setNumberValue(1000).build()))
            .build();
    String struct1Str = struct1.toString();
    SingleOutputStreamOperator<Struct1> stream =
        env.fromCollection(List.of(struct1))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Struct1>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        (TimestampAssignerSupplier<Struct1>)
                            context ->
                                (TimestampAssigner<Struct1>)
                                    (struct11, l) ->
                                        (long) struct11.getFieldsOrThrow("ts").getNumberValue()));
    FlinkTableUtils.registerProtoView(tEnv, stream, "Struct1View", true);
    Table structTable = tEnv.sqlQuery("SELECT * FROM Struct1View");
    DataStream<Struct1> resultStream =
        FlinkTableUtils.toProtoStreamWithRowtime(tEnv, structTable, Struct1.class, null);
    resultStream.process(
        new ProcessFunction<>() {
          @Override
          public void processElement(
              Struct1 struct,
              ProcessFunction<Struct1, Object>.Context context,
              Collector<Object> collector) {
            assertThat(struct.toString()).isEqualTo(struct1Str);
            assertThat(context.timestamp()).isEqualTo(1000L);
          }
        });
    env.execute("Proto Conversion Test");
  }
}
