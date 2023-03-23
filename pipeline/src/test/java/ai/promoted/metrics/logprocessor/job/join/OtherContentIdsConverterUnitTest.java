package ai.promoted.metrics.logprocessor.job.join;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.promoted.proto.common.Properties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class OtherContentIdsConverterUnitTest {

  @Test
  public void hasKeys() {
    assertFalse(new OtherContentIdsConverter(ImmutableList.of()).hasKeys());
    assertTrue(new OtherContentIdsConverter(ImmutableList.of("a")).hasKeys());
  }

  @Test
  public void putFromProperties_noKeys_noProperties() {
    Map<Integer, String> map = new HashMap<>();
    new OtherContentIdsConverter(ImmutableList.of())
        .putFromProperties(map::put, Properties.getDefaultInstance());
    assertEquals(0, map.size());
  }

  @Test
  public void putFromProperties_noKeys_properties() {
    Map<Integer, String> map = new HashMap<>();
    Properties properties =
        Properties.newBuilder()
            .setStruct(
                Struct.newBuilder()
                    .putFields("store", Value.newBuilder().setStringValue("store2").build())
                    .putFields("owner", Value.newBuilder().setStringValue("owner2").build()))
            .build();
    new OtherContentIdsConverter(ImmutableList.of()).putFromProperties(map::put, properties);
    assertEquals(0, map.size());
  }

  @Test
  public void putFromProperties_storeProp() {
    Map<Integer, String> map = new HashMap<>();
    Properties properties =
        Properties.newBuilder()
            .setStruct(
                Struct.newBuilder()
                    .putFields("store", Value.newBuilder().setStringValue("store2").build())
                    .putFields("owner", Value.newBuilder().setStringValue("owner2").build()))
            .build();
    new OtherContentIdsConverter(ImmutableList.of("store")).putFromProperties(map::put, properties);
    assertEquals(ImmutableMap.of(109770977, "store2"), map);
  }

  @Test
  public void putFromProperties_multipleProps() {
    Map<Integer, String> map = new HashMap<>();
    Properties properties =
        Properties.newBuilder()
            .setStruct(
                Struct.newBuilder()
                    .putFields("store", Value.newBuilder().setStringValue("store2").build())
                    .putFields("owner", Value.newBuilder().setStringValue("owner2").build()))
            .build();
    new OtherContentIdsConverter(ImmutableList.of("store", "owner"))
        .putFromProperties(map::put, properties);
    assertEquals(ImmutableMap.of(106164915, "owner2", 109770977, "store2"), map);
  }

  @Test
  public void putFromProperties_number() {
    Map<Integer, String> map = new HashMap<>();
    Properties properties =
        Properties.newBuilder()
            .setStruct(
                Struct.newBuilder()
                    .putFields("store", Value.newBuilder().setNumberValue(2.3).build()))
            .build();
    new OtherContentIdsConverter(ImmutableList.of("store")).putFromProperties(map::put, properties);
    assertEquals(ImmutableMap.of(109770977, "2.3"), map);
  }

  @Test
  public void putFromProperties_boolean() {
    Map<Integer, String> map = new HashMap<>();
    Properties properties =
        Properties.newBuilder()
            .setStruct(
                Struct.newBuilder()
                    .putFields("store", Value.newBuilder().setBoolValue(true).build()))
            .build();
    new OtherContentIdsConverter(ImmutableList.of("store")).putFromProperties(map::put, properties);
    assertEquals(ImmutableMap.of(109770977, "true"), map);
  }
}
