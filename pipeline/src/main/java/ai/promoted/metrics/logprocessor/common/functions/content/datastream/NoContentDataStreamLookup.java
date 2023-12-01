package ai.promoted.metrics.logprocessor.common.functions.content.datastream;

import java.util.Collections;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/** Do not do any lookups. */
public final class NoContentDataStreamLookup<T> extends RichAsyncFunction<T, T> {

  @Override
  public void asyncInvoke(T event, final ResultFuture<T> resultFuture) {
    resultFuture.complete(Collections.singleton(event));
  }
}
