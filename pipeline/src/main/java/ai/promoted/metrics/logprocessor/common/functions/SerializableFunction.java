package ai.promoted.metrics.logprocessor.common.functions;

import java.io.Serializable;
import java.util.function.Function;

public interface SerializableFunction<L, R> extends Function<L, R>, Serializable {}
