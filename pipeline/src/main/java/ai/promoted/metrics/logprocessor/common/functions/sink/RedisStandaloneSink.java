package ai.promoted.metrics.logprocessor.common.functions.sink;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Sink for non-clustered Redis instances. */
public class RedisStandaloneSink extends RedisSink {
  private static final Logger LOGGER = LogManager.getLogger(RedisStandaloneSink.class);

  /** Constructs a redis sink hitting address (e.g. 'redis://localhost:6399/0') */
  public RedisStandaloneSink(String address) {
    super(address);
  }

  @Override
  public void initConnection() {
    LOGGER.info("Opening standalone redis connection to {}", uri);
    RedisClient redisClient = RedisClient.create(uri);
    // We want to loudly fail in order to alert.
    redisClient.setOptions(
        ClientOptions.builder()
            .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
            .build());
    connection = redisClient.connect();
    client = redisClient;
  }

  @Override
  protected RedisClusterCommands<String, String> syncCommands() {
    return ((StatefulRedisConnection<String, String>) connection).sync();
  }

  @Override
  protected RedisClusterAsyncCommands<String, String> asyncCommands() {
    return ((StatefulRedisConnection<String, String>) connection).async();
  }
}
