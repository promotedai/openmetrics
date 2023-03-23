package ai.promoted.metrics.logprocessor.common.functions.content.datastream;

import ai.promoted.proto.event.TinyEvent;
import java.util.Collections;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/** Do not do any lookups. */
public final class NoContentDataStreamLookup extends RichAsyncFunction<TinyEvent, TinyEvent> {

  @Override
  public void asyncInvoke(TinyEvent event, final ResultFuture<TinyEvent> resultFuture) {
    resultFuture.complete(Collections.singleton(event));
  }
}
