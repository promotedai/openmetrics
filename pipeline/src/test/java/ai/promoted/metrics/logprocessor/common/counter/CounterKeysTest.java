package ai.promoted.metrics.logprocessor.common.counter;

import ai.promoted.metrics.logprocessor.common.counter.CounterKeys.CountKey;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import org.junit.jupiter.api.Test;

public class CounterKeysTest {

  @Test
  public void serializeCounterKeys() throws Exception {
    CounterKeys keys = new CounterKeys(true);
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      for (CountKey<?> key : keys.getAllKeys().values()) {
        oos.writeObject(key);
      }
    }
  }
}
