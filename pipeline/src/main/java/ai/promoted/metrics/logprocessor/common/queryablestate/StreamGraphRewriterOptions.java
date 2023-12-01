package ai.promoted.metrics.logprocessor.common.queryablestate;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class StreamGraphRewriterOptions {

  public static final ConfigOption<Integer> STATE_QUERY_PORT =
      ConfigOptions.key("promoted.state.query.port")
          .intType()
          .defaultValue(12345)
          .withDescription("The port used for the state query endpoint.");
}
