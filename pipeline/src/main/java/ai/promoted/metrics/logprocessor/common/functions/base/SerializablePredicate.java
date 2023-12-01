package ai.promoted.metrics.logprocessor.common.functions.base;

import java.io.Serializable;
import java.util.function.Predicate;

public interface SerializablePredicate<T> extends Predicate<T>, Serializable {}
