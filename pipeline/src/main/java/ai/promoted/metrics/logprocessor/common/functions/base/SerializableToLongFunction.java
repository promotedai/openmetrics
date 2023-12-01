package ai.promoted.metrics.logprocessor.common.functions.base;

import java.io.Serializable;
import java.util.function.ToLongFunction;

/** For performance to avoid unboxing. */
public interface SerializableToLongFunction<T> extends ToLongFunction<T>, Serializable {}
