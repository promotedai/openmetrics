package ai.promoted.metrics.logprocessor.common.fakedatagenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class IncrementingUUIDSupplierTest {

  // It's a lot of work to assert full properties.  Other tests do that .
  @Test
  public void get() {
    IncrementingUUIDSupplier supplier =
        new IncrementingUUIDSupplier("11111111-1111-1111-0000-000000000000");
    assertEquals("11111111-1111-1111-0000-000000000001", supplier.get());
    assertEquals("11111111-1111-1111-0000-000000000002", supplier.get());
    assertEquals("11111111-1111-1111-0000-000000000003", supplier.get());
  }
}
