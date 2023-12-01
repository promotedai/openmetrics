package ai.promoted.metrics.logprocessor.common.functions.base;

import java.io.Serializable;
import java.util.function.BiFunction;

public interface SerializableBiFunction<L1, L2, R> extends BiFunction<L1, L2, R>, Serializable {}
