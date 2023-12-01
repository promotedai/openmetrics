package ai.promoted.metrics.logprocessor.common.functions.sink;

import static io.lettuce.core.protocol.CommandType.ZADD;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableBiFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableConsumer;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableSupplier;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink.RedisSinkCommand;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.FlushMode;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.protocol.CommandType;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RedisSink extends RichSinkFunction<RedisSinkCommand> implements CheckpointedFunction {
  public static final int REDIS_WAIT_TIMEOUT = 60000; // 1 min
  // This is the ASCII unit separator (0x1f) character.
  public static final String JOIN_CHAR = Character.toString((char) 31);
  private static final Logger LOGGER = LogManager.getLogger(RedisSink.class);
  // Comma separated URI values: 'redis://host1:6399,host2:6399,...'
  protected final String uri;
  protected AbstractRedisClient client;
  protected StatefulConnection<String, String> connection;
  //  protected AtomicInteger nRemainingAsync = new AtomicInteger(0);
  protected AtomicInteger nRemainingAsync = new AtomicInteger(0);
  protected List<RedisSinkCommand> metadataCommands;
  private ScheduledExecutorService redisFlushService;
  private volatile boolean shouldFail = false;

  /**
   * Constructs a redis sink hitting a redis cluster
   *
   * @param uri a comma separated URI list: 'redis://host1:6399,host2:6399,...'
   * @param metadataCommands the metadata that needs to be written to Redis in each checkpoint
   */
  public RedisSink(String uri, List<RedisSinkCommand> metadataCommands) {
    this.uri = uri;
    this.metadataCommands = metadataCommands;
  }

  public static RedisSinkCommand hset(String key, String field, String value, long ttl) {
    return RedisSinkCommand.hset(key, field, value, ttl);
  }

  public static RedisSinkCommand hdel(String key, String field) {
    return RedisSinkCommand.hdel(key, field);
  }

  public static RedisSinkCommand hdel(Tuple key, Tuple field) {
    return hdel(tupleJoin(key), tupleJoin(field));
  }

  public static RedisSinkCommand hsetOrDel(Tuple key, Tuple field, Long value, long ttl) {
    if (value != 0) {
      return hset(key, field, value, ttl);
    } else {
      return hdel(key, field);
    }
  }

  public static RedisSinkCommand hset(Tuple key, Tuple field, String value, long ttl) {
    return hset(tupleJoin(key), tupleJoin(field), value, ttl);
  }

  public static RedisSinkCommand hset(Tuple key, Tuple field, long value, long ttl) {
    return hset(key, field, Long.toString(value), ttl);
  }

  public static RedisSinkCommand hset(Tuple key, Tuple field, Tuple value, long ttl) {
    return hset(tupleJoin(key), tupleJoin(field), tupleJoin(value), ttl);
  }

  public static RedisSinkCommand flush() {
    return RedisSinkCommand.forCommandType(CommandType.FLUSHDB);
  }

  public static RedisSinkCommand ping() {
    return RedisSinkCommand.forCommandType(CommandType.PING);
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
    initConnection();
    connection.setAutoFlushCommands(false);
    redisFlushService = Executors.newSingleThreadScheduledExecutor();
    redisFlushService.scheduleAtFixedRate(() -> connection.flushCommands(), 0, 1, TimeUnit.SECONDS);
    if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
      writeMetadata();
      waitForFlush(REDIS_WAIT_TIMEOUT);
    }
  }

  public void closeConnection(boolean waitForFlush) {
    if (waitForFlush) {
      waitForFlush(REDIS_WAIT_TIMEOUT);
    }
    if (null != redisFlushService) {
      redisFlushService.shutdown();
    }
    if (null != connection) {
      connection.close();
    }
    if (null != client) {
      client.shutdown();
    }
  }

  private void waitForFlush(long timeout) {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor
          .submit(
              () -> {
                while (nRemainingAsync.get() > 0) {
                  LOGGER.warn(
                      String.format(
                          "Waiting for %s Redis commands to flush.", nRemainingAsync.get()));
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                }
              })
          .get(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOGGER.error("Error while waiting for Redis commands to flush", e);
      throw new RuntimeException(e);
    } finally {
      if (!executor.isShutdown()) {
        executor.shutdown();
      }
    }
  }

  @Override
  public void close() throws Exception {
    closeConnection(true);
  }

  protected RedisClusterCommands<String, String> syncCommands() {
    return ((StatefulRedisClusterConnection<String, String>) connection).sync();
  }

  protected RedisClusterAsyncCommands<String, String> asyncCommands() {
    return ((StatefulRedisClusterConnection<String, String>) connection).async();
  }

  public void sendCommand(RedisSinkCommand in) {
    switch (in.getCommand()) {
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

  public void sendAsyncCommand(RedisSinkCommand in) {
    if (shouldFail) {
      throw new RuntimeException(
          "Some commands are failed to be flushed. Will trigger a job failover to retry...");
    }
    // We only will log exceptions and never block (sync on the response futures).
    RedisClusterAsyncCommands<String, String> async = asyncCommands();
    nRemainingAsync.incrementAndGet();
    //    RedisFuture<?> commandFuture;
    SerializableSupplier<RedisFuture<?>> futureSupplier;
    switch (in.getCommand()) {
      case HSET:
        futureSupplier = () -> async.hset(in.getKey(), in.getField(), in.getValue());
        break;
      case HDEL:
        assert (null == in.getValue()); // The value must be null for HDEL.
        futureSupplier = () -> async.hdel(in.getKey(), in.getField());
        break;
      case ZADD:
        futureSupplier =
            () -> async.zadd(in.getKey(), null, Double.parseDouble(in.getField()), in.getValue());
        break;
      case ZREM:
        futureSupplier = () -> async.zrem(in.getKey(), in.getValue());
        break;
      default:
        throw new IllegalArgumentException("Unrecognized RedisSinkCommand: " + in);
    }
    futureSupplier
        .get()
        .handle(
            (SerializableBiFunction<Object, Throwable, Object>)
                (o, throwable) -> {
                  if (null == throwable) {
                    nRemainingAsync.decrementAndGet();
                    return o;
                  } else {
                    LOGGER.error(
                        "redis failure for command {}: {}\n Will retry the same command synchronously...",
                        in,
                        throwable);
                    try {
                      // Retry once by running the command synchronously.
                      Object result = futureSupplier.get().get();
                      nRemainingAsync.decrementAndGet();
                      return result;
                    } catch (InterruptedException | ExecutionException e) {
                      LOGGER.error("Error while retrying the command {}: {}", in, e);
                      return null;
                    }
                  }
                })
        .thenAccept(
            (SerializableConsumer<Object>)
                result -> {
                  if (null == result) { // Null means a failure.
                    shouldFail = true;
                  }
                });

    // Set expiry TTL if positive.
    if (in.getTtl() > 0) {
      nRemainingAsync.incrementAndGet();
      async
          .expire(in.getKey(), in.getTtl())
          .handle(
              (SerializableBiFunction<Object, Throwable, Object>)
                  (o, throwable) -> {
                    if (null == throwable) {
                      nRemainingAsync.decrementAndGet();
                      return o;
                    } else {
                      LOGGER.error(
                          "redis failure for expire ({}, {}): {}. \n Will retry it synchronously...",
                          in.getKey(),
                          in.getTtl(),
                          throwable);
                      try {
                        // Retry once by running expire synchronously.
                        Boolean result = async.expire(in.getKey(), in.getTtl()).get();
                        nRemainingAsync.decrementAndGet();
                        return result;
                      } catch (InterruptedException | ExecutionException e) {
                        LOGGER.error(
                            "Error while retrying ({}, {}): {}", in.getKey(), in.getTtl(), e);
                        return null;
                      }
                    }
                  })
          .thenAccept(
              (SerializableConsumer<Object>)
                  result -> {
                    if (null == result) { // Null means a failure.
                      shouldFail = true;
                    }
                  });
      ;
      // Clear expiry if negative.
    } else if (in.getTtl() < 0) {
      nRemainingAsync.incrementAndGet();
      async
          .persist(in.getKey())
          .handle(
              (SerializableBiFunction<Object, Throwable, Object>)
                  (o, throwable) -> {
                    if (null == throwable) {
                      nRemainingAsync.decrementAndGet();
                      return o;
                    } else {
                      LOGGER.error(
                          "redis failure for persist ({}): {}. \n Will retry it synchronously...",
                          in.getKey(),
                          throwable);
                      try {
                        // Retry once more by running persist synchronously.
                        Boolean result = async.persist(in.getKey()).get();
                        nRemainingAsync.decrementAndGet();
                        return result;
                      } catch (InterruptedException | ExecutionException e) {
                        LOGGER.error("Error while retrying persist ({}): {}", in.getKey(), e);
                        return null;
                      }
                    }
                  })
          .thenAccept(
              (SerializableConsumer<Object>)
                  result -> {
                    if (null == result) { // Null means a failure.
                      shouldFail = true;
                    }
                  });
      ;
    } // else 0 ttl means don't mess with it!
  }

  @Override
  public void invoke(RedisSinkCommand in, Context context) throws Exception {
    // Only support async operations from within Flink.
    sendAsyncCommand(in);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {
    if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
      writeMetadata();
    }
    waitForFlush(REDIS_WAIT_TIMEOUT);
  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext) {
    // nothing to do
  }

  private void writeMetadata() {
    if (null != metadataCommands) {
      for (RedisSinkCommand cmd : metadataCommands) {
        sendCommand(cmd);
      }
    }
  }

  public static class RedisSinkCommand extends Tuple5<CommandType, String, String, String, Long> {

    public static KeySelector<RedisSinkCommand, Tuple2<String, String>> REDIS_HASH_KEY =
        new KeySelector<>() {
          @Override
          public Tuple2<String, String> getKey(RedisSinkCommand in) {
            return Tuple2.of(in.getKey(), in.getField());
          }
        };

    // This is for Flink serialization
    public RedisSinkCommand() {
      super();
    }

    private RedisSinkCommand(
        CommandType commandType, String key, String field, String value, Long ttl) {
      super(commandType, key, field, value, ttl);
    }

    public static RedisSinkCommand zadd(String key, String value, long score) {
      // TODO More fields in RedisSinkCommand
      return new RedisSinkCommand(ZADD, key, String.valueOf(score), value, 0L);
    }

    public static RedisSinkCommand zrem(String key, String value) {
      return new RedisSinkCommand(CommandType.ZREM, key, null, value, 0L);
    }

    public static RedisSinkCommand hset(String key, String field, String value, Long ttl) {
      return new RedisSinkCommand(CommandType.HSET, key, field, value, ttl);
    }

    public static RedisSinkCommand hdel(String key, String field) {
      return new RedisSinkCommand(CommandType.HDEL, key, field, null, -1L);
    }

    public static RedisSinkCommand forCommandType(CommandType commandType) {
      return new RedisSinkCommand(commandType, null, null, null, -1L);
    }

    public CommandType getCommand() {
      return f0;
    }

    public String getKey() {
      return f1;
    }

    public String getField() {
      return f2;
    }

    public String getValue() {
      return f3;
    }

    public Long getTtl() {
      return f4;
    }
  }
}
