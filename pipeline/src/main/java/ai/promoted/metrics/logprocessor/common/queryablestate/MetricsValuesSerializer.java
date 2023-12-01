package ai.promoted.metrics.logprocessor.common.queryablestate;

import ai.promoted.proto.flinkqueryablestate.MetricsValues;
import java.io.IOException;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class MetricsValuesSerializer extends TypeSerializerSingleton<MetricsValues> {
  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public MetricsValues createInstance() {
    return MetricsValues.newBuilder().build();
  }

  @Override
  public MetricsValues copy(MetricsValues metricsValues) {
    return metricsValues;
  }

  @Override
  public MetricsValues copy(MetricsValues metricsValues, MetricsValues t1) {
    return metricsValues;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(MetricsValues metricsValues, DataOutputView dataOutputView)
      throws IOException {
    byte[] bytes = metricsValues.toByteArray();
    dataOutputView.writeInt(bytes.length);
    dataOutputView.write(bytes);
  }

  @Override
  public MetricsValues deserialize(DataInputView dataInputView) throws IOException {
    int length = dataInputView.readInt();
    byte[] bytes = new byte[length];
    dataInputView.read(bytes);
    return MetricsValues.parseFrom(bytes);
  }

  @Override
  public MetricsValues deserialize(MetricsValues metricsValues, DataInputView dataInputView)
      throws IOException {
    return deserialize(dataInputView);
  }

  @Override
  public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
    int length = dataInputView.readInt();
    dataOutputView.write(length);
    dataOutputView.write(dataInputView, length);
  }

  @Override
  public TypeSerializerSnapshot<MetricsValues> snapshotConfiguration() {
    return new MetricsValuesSerializerSnapshot();
  }

  public static final class MetricsValuesSerializerSnapshot
      extends SimpleTypeSerializerSnapshot<MetricsValues> {
    public MetricsValuesSerializerSnapshot() {
      super(MetricsValuesSerializer::new);
    }
  }
}
