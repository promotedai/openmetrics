package ai.promoted.metrics.logprocessor.common.functions.content.datastream;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RotationBulkLoader<Key, Value> extends BulkLoader<Key, Value> implements Closeable {
  private static final Logger LOG = LogManager.getLogger(RotationBulkLoader.class);
  private volatile LinkedBlockingQueue<WaitingKey<Key, Value>> currentQueue =
      new LinkedBlockingQueue<>();
  private final ExecutorService loadingExecutor = Executors.newCachedThreadPool();
  private final ScheduledExecutorService schedulerExecutor =
      Executors.newSingleThreadScheduledExecutor();
  private final int maxBatchSize;
  private final Consumer<Collection<WaitingKey<Key, Value>>> loader;

  public RotationBulkLoader(
      int maxBatchSize, long maxDelayMills, Consumer<Collection<WaitingKey<Key, Value>>> loader) {
    this.maxBatchSize = maxBatchSize;
    this.loader = loader;
    schedulerExecutor.scheduleAtFixedRate(
        () -> submitLoadingTask(true), 0, maxDelayMills, TimeUnit.MILLISECONDS);
  }

  @Override
  public void addWaitingKey(WaitingKey<Key, Value> waitingKey) {
    currentQueue.add(waitingKey);
    if (currentQueue.size() >= maxBatchSize) {
      submitLoadingTask(false);
    }
  }

  private void submitLoadingTask(boolean force) {
    LinkedBlockingQueue<WaitingKey<Key, Value>> oldQueue = rotateForLoading(force);
    if (null != oldQueue && !oldQueue.isEmpty()) {
      loadingExecutor.submit(() -> loader.accept(oldQueue));
    }
  }

  private synchronized LinkedBlockingQueue<WaitingKey<Key, Value>> rotateForLoading(boolean force) {
    if (force || currentQueue.size() >= maxBatchSize) {
      LinkedBlockingQueue<WaitingKey<Key, Value>> oldQueue = currentQueue;
      currentQueue = new LinkedBlockingQueue<>();
      return oldQueue;
    }
    return null;
  }

  @Override
  public void close() {
    try {
      schedulerExecutor.shutdownNow();
    } catch (Exception ex) {
      LOG.warn(ex.getMessage());
    }
    try {
      loadingExecutor.shutdownNow();
    } catch (Exception ex) {
      LOG.warn(ex.getMessage());
    }
  }
}
