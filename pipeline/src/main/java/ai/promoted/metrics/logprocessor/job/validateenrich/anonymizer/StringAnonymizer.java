package ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;

/** Interface to anonymize a String. */
public interface StringAnonymizer extends SerializableFunction<String, String> {}
