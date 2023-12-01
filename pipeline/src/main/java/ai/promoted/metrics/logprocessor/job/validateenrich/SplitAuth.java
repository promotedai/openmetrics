package ai.promoted.metrics.logprocessor.job.validateenrich;

import ai.promoted.metrics.logprocessor.common.functions.FilterOperator;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.proto.common.UserInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

public class SplitAuth<T> extends FilterOperator<T> {
  public SplitAuth(SerializableFunction<T, UserInfo> getUserInfo, Class<T> clazz) {
    super(
        record -> !getUserInfo.apply(record).getUserId().isEmpty(),
        new OutputTag<>("unauth", TypeInformation.of(clazz)));
  }
}
