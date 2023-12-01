package ai.promoted.metrics.logprocessor.common.functions.base;

import java.io.Serializable;
import java.util.function.BiConsumer;

public interface SerializableBiConsumer<L1, L2> extends BiConsumer<L1, L2>, Serializable {}
