package ai.promoted.metrics.logprocessor.common.functions.sink;

import com.google.auto.value.AutoValue;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.FlushMode;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.protocol.CommandType;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RedisSink extends RichSinkFunction<RedisSink.Command> {
  // This is the ASCII unit separator (0x1f) character.
  public static final String JOIN_CHAR = Character.toString((char) 31);
  private static final Logger LOGGER = LogManager.getLogger(RedisSink.class);
  // Comma separated URI values: 'redis://host1:6399,host2:6399,...'
  protected final String uri;
  protected AbstractRedisClient client;
  protected StatefulConnection<String, String> connection;
  protected AtomicInteger nRemainingAsync = new AtomicInteger(0);

  /**
   * Constructs a redis sink hitting a redis cluster. Specify a comma separated URI list:
   * 'redis://host1:6399,host2:6399,...'
   */
  public RedisSink(String uri) {
    this.uri = uri;
  }

  public static Command hset(String key, String field, String value, long ttl) {
    return RedisSink.Command.builder()
        .setCommand(CommandType.HSET)
        .setKey(key)
        .setField(field)
        .setValue(value)
        .setTtl(ttl)
        .build();
  }

  public static Command hdel(String key, String field) {
    return RedisSink.Command.builder()
        .setCommand(CommandType.HDEL)
        .setKey(key)
        .setField(field)
        .build();
  }

  public static Command hdel(Tuple key, Tuple field) {
    return hdel(tupleJoin(key), tupleJoin(field));
  }

  public static Command hsetOrDel(Tuple key, Tuple field, Long value, long ttl) {
    if (value != 0) {
      return hset(key, field, value, ttl);
    } else {
      return hdel(key, field);
    }
  }

  public static Command hset(Tuple key, Tuple field, String value, long ttl) {
    return hset(tupleJoin(key), tupleJoin(field), value, ttl);
  }

  public static Command hset(Tuple key, Tuple field, long value, long ttl) {
    return hset(key, field, Long.toString(value), ttl);
  }

  public static Command hset(Tuple key, Tuple field, Tuple value, long ttl) {
    return hset(tupleJoin(key), tupleJoin(field), tupleJoin(value), ttl);
  }

  public static Command flush() {
    return RedisSink.Command.builder().setCommand(CommandType.FLUSHDB).build();
  }

  public static Command ping() {
    return RedisSink.Command.builder().setCommand(CommandType.PING).build();
  }

  /** Joins a Flink Tuple using a field delimiter (usually, 0x1f). */
  public static String tupleJoin(Tuple value) {
    StringJoiner joined = new StringJoiner(JOIN_CHAR);
    for (int i = 0; i < value.getArity(); ++i) {
      try {
        joined.add(value.getField(i).toString());
      } catch (Exception e) {
        throw new IllegalStateException("bad tuple: " + value, e);
      }
    }
    return joined.toString();
  }

  public void initConnection() {
    LOGGER.info("Opening clustered redis connection URI: {}", uri);
    RedisClusterClient clusterClient = RedisClusterClient.create(uri);
    clusterClient.setOptions(
        ClusterClientOptions.builder()
            // We want to loudly fail in order to alert.
            .disconnectedBehavior(ClusterClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
            // https://github.com/lettuce-io/lettuce-core/issues/339
            // TODO: enable periodic if we hit any edge cases.
            .topologyRefreshOptions(
                ClusterTopologyRefreshOptions.builder().enableAllAdaptiveRefreshTriggers().build())
            .build());
    connection = clusterClient.connect();
    client = clusterClient;
  }

  @Override
  public void open(Configuration config) throws Exception {
    super.open(config);
    initConnection();
  }

  public void closeConnection(boolean waitForAsync) throws InterruptedException {
    int nSleeps = 20; // * 500ms = 10s max busy waiting
    while (waitForAsync && nRemainingAsync.get() > 0 && nSleeps > 0) {
      Thread.sleep(500);
      nSleeps--;
    }
    connection.close();
    client.shutdown();
  }

  @Override
  public void close() throws Exception {
    closeConnection(false);
    super.close();
  }

  protected RedisClusterCommands<String, String> syncCommands() {
    return ((StatefulRedisClusterConnection<String, String>) connection).sync();
  }

  protected RedisClusterAsyncCommands<String, String> asyncCommands() {
    return ((StatefulRedisClusterConnection<String, String>) connection).async();
  }

  public void sendCommand(Command in) {
    switch (in.command()) {
      case FLUSHDB:
        LOGGER.warn("flushing redis instance!");
        syncCommands().flushdb(FlushMode.SYNC);
        return;
      case PING:
        LOGGER.warn("pinging redis instance!");
        String reply = syncCommands().ping();
        LOGGER.warn("redis ping reply: {}", reply);
        return;
      default:
        sendAsyncCommand(in);
    }
  }

  public void sendAsyncCommand(Command in) {
    // We only will log exceptions and never block (sync on the response futures).
    RedisClusterAsyncCommands<String, String> async = asyncCommands();
    async.setAutoFlushCommands(false);

    switch (in.command()) {
      case HINCRBY:
        nRemainingAsync.incrementAndGet();
        if (in.value() instanceof Long) {
          async
              .hincrby(in.key(), in.field(), (Long) in.value())
              .exceptionally(
                  e -> {
                    LOGGER.error("redis failure for command {}: {}", in, e);
                    return -1L;
                  })
              .thenRun(() -> nRemainingAsync.decrementAndGet());
        } else {
          LOGGER.error("illegal Long value for command {}", in);
        }
        break;
      case HSET:
        nRemainingAsync.incrementAndGet();
        if (in.value() instanceof String) {
          async
              .hset(in.key(), in.field(), (String) in.value())
              .exceptionally(
                  e -> {
                    LOGGER.error("redis failure for command {}: {}", in, e);
                    return false;
                  })
              .thenRun(() -> nRemainingAsync.decrementAndGet());
        } else {
          LOGGER.error("illegal String value for command {}", in);
        }
        break;
      case HDEL:
        nRemainingAsync.incrementAndGet();
        if (in.value() instanceof String) {
          async
              .hdel(in.key(), in.field())
              .exceptionally(
                  e -> {
                    LOGGER.error("redis failure for command {}: {}", in, e);
                    return 0L;
                  })
              .thenRun(() -> nRemainingAsync.decrementAndGet());
        } else {
          LOGGER.error("illegal String value for command {}", in);
        }
        break;
      default:
        throw new IllegalArgumentException("unrecognized RedisSink.Command: " + in);
    }
    // Set expiry TTL if positive.
    if (in.ttl() > 0) {
      nRemainingAsync.incrementAndGet();
      async
          .expire(in.key(), in.ttl())
          .exceptionally(
              e -> {
                LOGGER.error("redis failure for expire ({}, {}): {}", in.key(), in.ttl(), e);
                return false;
              })
          .thenRun(() -> nRemainingAsync.decrementAndGet());
      // Clear expiry if negative.
    } else if (in.ttl() < 0) {
      nRemainingAsync.incrementAndGet();
      async
          .persist(in.key())
          .exceptionally(
              e -> {
                LOGGER.error("redis failure for persist ({}): {}", in.key(), e);
                return false;
              })
          .thenRun(() -> nRemainingAsync.decrementAndGet());
    } // else 0 ttl means don't mess with it!
    async.flushCommands();
  }

  @Override
  public void invoke(Command in, Context context) throws Exception {
    super.invoke(in, context);
    // Only support async operations from within Flink.
    sendAsyncCommand(in);
  }

  // TODO: use lettuce's Command directly.
  @AutoValue
  public abstract static class Command {
    public static KeySelector<Command, Tuple2<String, String>> REDIS_HASH_KEY =
        new KeySelector<>() {
          @Override
          public Tuple2<String, String> getKey(Command in) {
            return Tuple2.of(in.key(), in.field());
          }
        };

    static Builder builder() {
      return new AutoValue_RedisSink_Command.Builder().setTtl(-1);
    }

    public abstract CommandType command();

    @Nullable
    public abstract String key();

    @Nullable
    public abstract String field();

    @Nullable
    public abstract Object value();

    public abstract long ttl();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCommand(CommandType cmd);

      abstract Builder setKey(String key);

      abstract Builder setField(String field);

      abstract Builder setValue(Object value);

      abstract Builder setTtl(long ttl);

      abstract Command build();
    }
  }
}
