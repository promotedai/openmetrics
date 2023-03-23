package ai.promoted.metrics.logprocessor.common.util;

import static ai.promoted.metrics.logprocessor.common.util.DeliveryLogUtil.getTrafficPriority;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.common.ClientInfo;
import org.junit.jupiter.api.Test;

public class DeliveryLogUtilTest {

  @Test
  public void catchNewTrafficType() throws Exception {
    for (ClientInfo.TrafficType trafficType : ClientInfo.TrafficType.values()) {
      // If trafficType is unsupported, the code will throw.
      getTrafficPriority(trafficType);
    }
  }

  @Test
  public void shouldJoin_unsupportedEnumValue() {
    assertEquals(
        7,
        ClientInfo.TrafficType.values().length,
        "If you need to update this number, make sure shouldJoin is updated. "
            + "Java proto enums add +1 for UNRECOGNIZED.");
  }
}
