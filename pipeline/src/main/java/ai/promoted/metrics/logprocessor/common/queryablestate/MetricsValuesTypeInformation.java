package ai.promoted.metrics.logprocessor.common.queryablestate;

import ai.promoted.proto.flinkqueryablestate.MetricsValues;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class MetricsValuesTypeInformation extends TypeInformation<MetricsValues> {

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 1;
  }

  @Override
  public int getTotalFields() {
    return 1;
  }

  @Override
  public Class<MetricsValues> getTypeClass() {
    return MetricsValues.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<MetricsValues> createSerializer(ExecutionConfig executionConfig) {
    return new MetricsValuesSerializer();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof MetricsValuesTypeInformation;
  }

  @Override
  public int hashCode() {
    return MetricsValuesTypeInformation.class.hashCode();
  }

  @Override
  public boolean canEqual(Object o) {
    return o instanceof MetricsValuesTypeInformation;
  }
}
