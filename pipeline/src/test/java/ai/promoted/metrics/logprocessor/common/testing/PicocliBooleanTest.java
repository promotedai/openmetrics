package ai.promoted.metrics.logprocessor.common.testing;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PicocliBooleanTest {
    static class App {
        @Option(names = "--startsFalse", negatable = true) boolean startsFalse = false;
        @Option(names = "--no-startsTrue", negatable = true) boolean startsTrue = true;
    }

    App app;

    @BeforeEach
    void initApp() {
        app = new App();
    }

    @Test
    public void testPreParsed() {
        assertFalse(app.startsFalse);
        assertTrue(app.startsTrue);
    }

    @Test
    public void testNoArgs() {
        new CommandLine(app).parseArgs();
        assertFalse(app.startsFalse);
        assertTrue(app.startsTrue);
    }

    @Test
    public void testBothSpecifiedTrue() {
        new CommandLine(app).parseArgs("--startsFalse", "--startsTrue");
        assertTrue(app.startsFalse);
        assertTrue(app.startsTrue);
    }

    @Test
    public void testBothSpecifiedNegated() {
        new CommandLine(app).parseArgs("--no-startsFalse", "--no-startsTrue");
        assertFalse(app.startsFalse);
        assertFalse(app.startsTrue);
    }

    @Test
    public void testBothExplicitlyTrue() {
        new CommandLine(app).parseArgs("--startsFalse=true", "--startsTrue=true");
        assertTrue(app.startsFalse);
        assertTrue(app.startsTrue);
    }

    @Test
    public void testBothExplicityFalse() {
        new CommandLine(app).parseArgs("--startsFalse=false", "--no-startsTrue=false");
        assertFalse(app.startsFalse);
        assertFalse(app.startsTrue);
    }
}
