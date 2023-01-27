package ai.promoted.metrics.logprocessor.common.fakedatagenerator;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * A Supplier that returns an incrementing UUID that also contains a prefix.
 */
class StringPrefixUUIDSupplier implements Supplier<String> {
    private final String prefix;
    private final Supplier<Long> incrementingSupplier;

    public StringPrefixUUIDSupplier(String prefix) {
        this.prefix = prefix;
        incrementingSupplier = new AtomicLong(0)::incrementAndGet;
    }

    @Override
    public String get() {
        return prefix + incrementingSupplier.get();
    }
}
