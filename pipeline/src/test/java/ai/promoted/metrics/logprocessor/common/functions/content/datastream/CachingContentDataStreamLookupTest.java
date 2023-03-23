package ai.promoted.metrics.logprocessor.common.functions.content.datastream;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.mockito.Mockito.verify;

import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.event.TinyEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Collections;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CachingContentDataStreamLookupTest {
  private CachingContentDataStreamLookup lookup;
  private ResultFuture<TinyEvent> future;

  @BeforeEach
  public void setUp() {
    lookup =
        new CachingContentDataStreamLookup(
            100,
            Duration.ofSeconds(10),
            (contentIds) -> {
              assertIterableEquals(ImmutableList.of("content1"), contentIds);
              return ImmutableMap.of("content1", ImmutableMap.of("parent1", "value1"));
            },
            ImmutableSet.of("parent1"),
            DebugIds.empty());
    future = Mockito.mock(ResultFuture.class);
  }

  @Test
  public void asyncInvoke_noContentId() {
    TinyEvent event = TinyEvent.getDefaultInstance();
    lookup.asyncInvoke(event, future);
    verify(future).complete(Collections.singleton(event));
  }

  @Test
  public void asyncInvoke() {
    TinyEvent event = TinyEvent.newBuilder().setContentId("content1").build();
    lookup.asyncInvoke(event, future);
    verify(future)
        .complete(
            Collections.singleton(
                event.toBuilder()
                    .putOtherContentIds(StringUtil.hash("parent1"), "value1")
                    .build()));
  }
}
