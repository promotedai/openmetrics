package ai.promoted.metrics.logprocessor.common.functions.content.datastream;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

public abstract class BulkLoader<Key, Value> implements Closeable {
  /** Adds a waiting key to the loader. */
  public CompletableFuture<Value> add(Key key) {
    final WaitingKey<Key, Value> waitingKey = new WaitingKey<>(key);
    addWaitingKey(waitingKey);
    return waitingKey.future;
  }

  abstract void addWaitingKey(WaitingKey<Key, Value> waitingKey);

  public static class WaitingKey<Key, Value> {
    public final Key key;
    public final CompletableFuture<Value> future = new CompletableFuture<>();

    public WaitingKey(Key key) {
      this.key = key;
    }
  }
}
