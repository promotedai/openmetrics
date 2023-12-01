package ai.promoted.metrics.logprocessor.common.functions.content.datastream;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.chill.protobuf.ProtobufSerializer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CachedContentLookupFunctionTest {
  private LinkedBlockingDeque<Boolean> triggered;
  private CachedContentLookupFunction<TinyInsertion, TinyInsertion.Builder> lookup;
  private ResultFuture<TinyInsertion> future;

  private AtomicInteger exceptionTimes;
  private static final String EXCEPTIONAL_CONTENT_ID = "exceptional_content_id";
  private static final String MISSING_CONTENT_ID = "missing_content_id";
  private static final Map<String, Map<String, Map<String, Object>>> CONTENT_MAP =
      ImmutableMap.of(
          "content1", ImmutableMap.of("defaultSource", ImmutableMap.of("parent1", "value1")));

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setUp() {
    triggered = new LinkedBlockingDeque<>(10);
    exceptionTimes = new AtomicInteger(0);
    ContentCacheSupplier contentCacheSupplier =
        new ContentCacheSupplier(
            100,
            1,
            Duration.ofMillis(100L),
            Duration.ofSeconds(10),
            (params) -> {
              // Immutable map doesn't support null values
              Map<String, Map<String, Map<String, Object>>> result = new HashMap<>();
              assertIterableEquals(ImmutableList.of("parent1"), params.projections());
              // Only fetch by contentIds and skip projections
              for (int i = 0; i < params.contentIds().size(); ++i) {
                String contentId = params.contentIds().get(i);
                if (contentId.equals(EXCEPTIONAL_CONTENT_ID)) {
                  if (exceptionTimes.get() > 0) {
                    exceptionTimes.decrementAndGet();
                    throw new ContentQueryException("Error whiling fetching contents");
                  }
                } else if (!MISSING_CONTENT_ID.equals(contentId)) {
                  Map<String, Map<String, Object>> mapForContent = CONTENT_MAP.get(contentId);
                  result.put(contentId, mapForContent);
                }
              }
              return result;
            },
            ImmutableSet.of("parent1"),
            supplier -> CompletableFuture.completedFuture(supplier.get()));
    lookup =
        new CachedContentLookupFunction<TinyInsertion, TinyInsertion.Builder>(
            contentCacheSupplier,
            TinyInsertion::newBuilder,
            TinyInsertion.Builder::build,
            insertion -> insertion.getCore().getContentId(),
            (insertion, contentId) -> insertion.getCore().containsOtherContentIds(contentId),
            (builder, entry) ->
                builder.getCoreBuilder().putOtherContentIds(entry.getKey(), entry.getValue()),
            (record) -> false);
    lookup.completionNotifier = var1 -> triggered.offer(true);
    future = Mockito.mock(ResultFuture.class);
  }

  @Test
  public void asyncInvokeWithoutContentId() {
    TinyInsertion event = TinyInsertion.getDefaultInstance();
    lookup.asyncInvoke(event, future);
    verify(future).complete(Collections.singleton(event));
  }

  @Test
  public void asyncInvoke() throws Exception {
    TinyInsertion.Builder eventBuilder =
        TinyInsertion.newBuilder().setCore(TinyInsertionCore.newBuilder().setContentId("content1"));
    lookup.asyncInvoke(eventBuilder.clone().build(), future);
    waitForDone();
    eventBuilder.getCoreBuilder().putOtherContentIds(StringUtil.hash("parent1"), "value1");
    verify(future, timeout(1000)).complete(Collections.singleton(eventBuilder.build()));
  }

  @Test
  public void asyncInvokeWithContentQueryException() throws Exception {
    TinyInsertion event =
        TinyInsertion.newBuilder()
            .setCore(TinyInsertionCore.newBuilder().setContentId(EXCEPTIONAL_CONTENT_ID))
            .build();
    exceptionTimes.incrementAndGet();
    lookup.asyncInvoke(event, future);
    waitForDone();
    verify(future)
        .completeExceptionally(new ContentQueryException("Error whiling fetching contents"));
  }

  @Test
  public void asyncInvokeWithMissingContents() throws InterruptedException {
    TinyInsertion event =
        TinyInsertion.newBuilder()
            .setCore(TinyInsertionCore.newBuilder().setContentId(MISSING_CONTENT_ID))
            .build();
    lookup.asyncInvoke(event, future);
    waitForDone();
    // Return the original event if fail to fetch the content
    verify(future, timeout(1000)).complete(Collections.singleton(event));
  }

  @Test
  public void asyncInvokeRetry() throws Exception {
    TinyInsertion event1 =
        TinyInsertion.newBuilder()
            .setCore(TinyInsertionCore.newBuilder().setContentId("content1"))
            .build();
    TinyInsertion event2 =
        TinyInsertion.newBuilder()
            .setCore(TinyInsertionCore.newBuilder().setContentId(MISSING_CONTENT_ID))
            .build();
    TinyInsertion event3 =
        TinyInsertion.newBuilder()
            .setCore(TinyInsertionCore.newBuilder().setContentId(EXCEPTIONAL_CONTENT_ID))
            .build();
    // Only throw exception twice
    exceptionTimes.set(2);
    try (OneInputStreamOperatorTestHarness<TinyInsertion, TinyInsertion> testHarness =
        createTestHarnessWithRetry(
            lookup, 10, 100, AsyncDataStream.OutputMode.UNORDERED, getAsyncRetryStrategy())) {
      testHarness.setup();
      testHarness
          .getExecutionConfig()
          .registerTypeWithKryoSerializer(TinyInsertion.class, ProtobufSerializer.class);
      testHarness.getOperator().open();
      testHarness.processElement(new StreamRecord<>(event1, 1));
      testHarness.processElement(new StreamRecord<>(event2, 2));
      // The exception should trigger a task failover.
      // However, MockEnvironment does not support external task failure.
      // testHarness.processElement(new StreamRecord<>(event3, 3));
      waitForDone();
      testHarness.endInput();
      List<TinyInsertion> result = testHarness.extractOutputValues();
      TinyInsertion.Builder expectedResult1Builder = event1.toBuilder();
      expectedResult1Builder
          .getCoreBuilder()
          .putOtherContentIds(StringUtil.hash("parent1"), "value1");
      assertThat(result).containsExactly(expectedResult1Builder.build(), event2);
    }
  }

  private <T> AsyncRetryStrategy<T> getAsyncRetryStrategy() {
    return new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<T>(10, 1)
        .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
        .build();
  }

  private OneInputStreamOperatorTestHarness<TinyInsertion, TinyInsertion>
      createTestHarnessWithRetry(
          AsyncFunction<TinyInsertion, TinyInsertion> function,
          long timeout,
          int capacity,
          AsyncDataStream.OutputMode outputMode,
          AsyncRetryStrategy<TinyInsertion> asyncRetryStrategy)
          throws Exception {
    ExecutionConfig executionConfig = new ExecutionConfig();
    executionConfig.registerTypeWithKryoSerializer(TinyInsertion.class, ProtobufSerializer.class);
    TypeSerializer<TinyInsertion> serializer =
        new KryoSerializer<>(TinyInsertion.class, executionConfig);
    return new OneInputStreamOperatorTestHarness<>(
        new AsyncWaitOperatorFactory<>(function, timeout, capacity, outputMode, asyncRetryStrategy),
        serializer);
  }

  private void waitForDone() throws InterruptedException {
    Boolean triggerResult = triggered.poll(5, TimeUnit.SECONDS);
    if (null == triggerResult || !triggerResult) {
      throw new IllegalStateException("callback should have triggered");
    }
  }

  /** The exception used for mockito verification */
  private static class ContentQueryException extends RuntimeException {
    public ContentQueryException(String message) {
      super(message);
    }

    @Override
    public int hashCode() {
      return this.getMessage().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ContentQueryException) {
        return Objects.equals(this.getMessage(), ((ContentQueryException) obj).getMessage());
      }
      return false;
    }
  }
}
