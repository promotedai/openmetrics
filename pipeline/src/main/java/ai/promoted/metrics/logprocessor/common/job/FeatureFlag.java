package ai.promoted.metrics.logprocessor.common.job;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Used to annotate picocli options that change features of the Flink job. Our unit tests will use
 * the annotated options and run tests with features turned off and on. The important options to
 * annotate are ones that add/remove operators.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface FeatureFlag {}
