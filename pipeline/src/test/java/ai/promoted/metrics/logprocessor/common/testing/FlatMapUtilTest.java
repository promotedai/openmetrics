package ai.promoted.metrics.logprocessor.common.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class FlatMapUtilTest {

  @Test
  public void empty() {
    assertEquals(ImmutableMap.of(), FlatMapUtil.flatten(ImmutableMap.of()));
  }

  @Test
  public void simple() {
    assertEquals(ImmutableMap.of("/key", 1), FlatMapUtil.flatten(ImmutableMap.of("key", 1)));
  }

  @Test
  public void listValue() {
    assertEquals(
        ImmutableMap.of("/key/0", "value1", "/key/1", 2),
        FlatMapUtil.flatten(ImmutableMap.of("key", ImmutableList.of("value1", 2))));
  }

  @Test
  public void nested() {
    assertEquals(
        ImmutableMap.of(
            "/key/0", "value1",
            "/key/1/key2/0", 2,
            "/key/1/key2/1/key3", "value3"),
        FlatMapUtil.flatten(
            ImmutableMap.of(
                "key",
                ImmutableList.of(
                    "value1",
                    ImmutableMap.of(
                        "key2", ImmutableList.of(2, ImmutableMap.of("key3", "value3")))))));
  }
}
