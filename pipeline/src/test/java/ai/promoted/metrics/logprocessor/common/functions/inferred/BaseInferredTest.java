package ai.promoted.metrics.logprocessor.common.functions.inferred;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

/**
 * Please look at the specific join tests in this package for unit tests for BaseInferred.
 * This test tests utility methods.
 */
public class BaseInferredTest {

    // Zero should not be called.  If we have a bug and call this method with a listSize=0,
    // we'll log some weird text for it.
    @ParameterizedTest
    @ValueSource(ints = {0, 500, 1000, 5000, 10000, 20000, 30000})
    public void matchesLongListThreshold_true(int listSize) {
        assertTrue(BaseInferred.matchesLongListThreshold(listSize));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 499, 501, 999, 1001, 4999, 5001, 9999, 10001, 10500, 11000, 15000,
            19999, 20001, 29999, 30001})
    public void matchesLongListThreshold_false(int listSize) {
        assertFalse(BaseInferred.matchesLongListThreshold(listSize));
    }
}
