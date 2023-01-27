package ai.promoted.metrics.logprocessor.common.testing;

import com.google.protobuf.Descriptors;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Proto Avro asserts.
 * Proto or Pojo specific asserts should go into other assert files.
 */
public class ProtoAsserts {

    public static void assertNoUnknownRecursiveTypes(Descriptors.Descriptor descriptor, Set<String> okayNestedTypes) {
        assertNoUnknownRecursiveTypes(descriptor, okayNestedTypes, new HashSet<String>());
    }

    // This won't catch cases where okayNestedTypes is stale and should have entries removed.
    private static void assertNoUnknownRecursiveTypes(Descriptors.Descriptor descriptor, Set<String> okayNestedTypes, Set<String> alreadyTraversedTypes) {
        String typeName = descriptor.getFullName();
        // Clone okayNestedTypes.  Each field path gets it's own okayNestedTypes.
        // This is a little slow but it's fine since it only runs in tests.
        alreadyTraversedTypes = new HashSet<>(alreadyTraversedTypes);
        if (alreadyTraversedTypes.contains(typeName)) {
            if (okayNestedTypes.contains(typeName)) {
                return; // Don't traverse further.
            } else {
                fail("record contained unexpected recursive type: " + typeName);
            }
        }
        alreadyTraversedTypes.add(typeName);

        for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
            switch (fieldDescriptor.getType()) {
                case MESSAGE:
                    assertNoUnknownRecursiveTypes(fieldDescriptor.getMessageType(), okayNestedTypes, alreadyTraversedTypes);
                    break;
            }
        }
    }
}
