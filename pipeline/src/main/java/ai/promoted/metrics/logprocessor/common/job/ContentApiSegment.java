package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.aws.AwsSecretsManagerClient;
import ai.promoted.metrics.logprocessor.common.functions.content.common.ContentQueryClient;
import ai.promoted.metrics.logprocessor.common.functions.content.common.ContentQueryClientImpl;
import ai.promoted.metrics.logprocessor.common.functions.content.datastream.CachedContentLookupFunction;
import ai.promoted.metrics.logprocessor.common.functions.content.datastream.ContentCacheSupplier;
import ai.promoted.metrics.logprocessor.common.functions.content.datastream.NoContentDataStreamLookup;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyInsertion;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Option;

public class ContentApiSegment implements FlinkSegment {
  private static final Logger LOGGER = LogManager.getLogger(ContentApiSegment.class);
  private final BaseFlinkJob baseFlinkJob;
  private final RegionSegment regionSegment;

  @Option(
      names = {"--enableInsertionContentLookup"},
      defaultValue = "false",
      description = "Enables the Content lookup for Insertions. Default=false.")
  public boolean enableInsertionContentLookup = false;

  @Option(
      names = {"--enableActionContentLookup"},
      defaultValue = "false",
      description = "Enables the Content lookup for Actions. Default=false.")
  public boolean enableActionContentLookup = false;

  @Option(
      names = {"--enableDummyContentApiLoader"},
      defaultValue = "false",
      description =
          "This can be enabled to create the Flink operator (with uids).  This will allow us to not get the drop state error when recovering from a savepoint. Default=false.")
  public boolean enableDummyContentApiLoader = false;

  @FeatureFlag
  @Option(
      names = {"--contentApiRootUrl"},
      defaultValue = "",
      description =
          "If set, turns on content joining.  Format matches `http://ee1a-67-168-60-51.ngrok.io/` or `http://localhost:5150/`.")
  public String contentApiRootUrl = "";

  @Option(
      names = {"--contentApiKey"},
      defaultValue = "",
      description = "The API Key for the Content API.  Default=empty string")
  public String contentApiKey = "";

  @Option(
      names = {"--contentLookupOperatorTimeout"},
      defaultValue = "PT10S",
      description = "Timeout for the RichAsyncFunction lookup. Default=PT10S.")
  public Duration contentLookupOperatorTimeout = Duration.parse("PT10S");

  @Option(
      names = {"--contentApiTimeout"},
      defaultValue = "PT3S",
      description = "Timeout for the Content API call. Default=PT3S.")
  public Duration contentApiTimeout = Duration.parse("PT3S");

  @Option(
      names = {"--contentApiSecretName"},
      defaultValue = "",
      description =
          "The AWS SecretsManager secret containing the Content API key.  Empty means do not use SecretsManager. Default=empty string.")
  public String contentApiSecretName = "";

  @Option(
      names = {"--contentApiSecretKey"},
      defaultValue = "api-key",
      description =
          "The key in the AWS SecretsManager secret to use for the API key.  Default=api-key.")
  public String contentApiSecretKey = "api-key";

  // TODO - future optimizations - we can split this into 3 flags to reduce the number of key
  // checks.
  // Splitting this would make the code more complex.
  @Option(
      names = {"--contentIdFieldKeys"},
      description =
          "Which fields from Content CMS and Properties should be used for other content IDs.  The order impacts the inferred reference join ordering.  Defaults to empty set.")
  public List<String> contentIdFieldKeys = new LinkedList<>();

  @Option(
      names = {"--contentApiCapacity"},
      defaultValue = "1000",
      description =
          "Maximum capacity setting for Content API.  This probably gets operated per client instance.  Default=1000.  Rough math is 100 batch size * ~10 concurrent RPCs.")
  public int contentApiCapacity = 1000;

  @Option(
      names = {"--contentCacheMaxSize"},
      defaultValue = "50000",
      description = "Maximum size of content cache.  Default=50k.")
  public int contentCacheMaxSize = 50000;

  @Option(
      names = {"--contentCacheMaxBatchSize"},
      defaultValue = "100",
      description =
          "Maximum batch size when filling the content cache.  This should be less than Content API batch limit.  Default=100.")
  public int contentCacheMaxBatchSize = 100;

  @Option(
      names = {"--contentLookupMaxAttempts"},
      defaultValue = "5",
      description =
          "Maximum number of Content API lookup attempts.  Handles networking issue.  Default=5.")
  public int contentLookupMaxAttempts = 5;

  @Option(
      names = {"--contentLookupFixedDelay"},
      defaultValue = "PT3S",
      description =
          "Wait time when an Exception is encountered when doing Content API lookups. Default=PT3S.")
  public Duration contentLookupFixedDelay = Duration.parse("PT3S");

  @Option(
      names = {"--contentCacheMaxBatchDelay"},
      defaultValue = "PT0.25S",
      description =
          "Max time to wait to fill up a batch when populating the cache. Default=PT0.25S to trade off batching and caches.")
  public Duration contentCacheMaxBatchDelay = Duration.parse("PT0.25S");

  @Option(
      names = {"--contentCacheExpiration"},
      defaultValue = "P1D",
      description = "Expiration time for content cache. Default=PT1D to capture global patterns.")
  public Duration contentCacheExpiration = Duration.parse("P1D");

  public ContentQueryClient contentQueryClient;

  RichAsyncFunction<TinyInsertion, TinyInsertion> overrideTinyInsertionOtherContentIds;
  RichAsyncFunction<TinyAction, TinyAction> overrideTinyActionOtherContentIds;
  ContentCacheSupplier contentCacheSupplier;

  @VisibleForTesting Supplier<String> getAwsSecret;

  private String secretStoreApiKey;

  public ContentApiSegment(BaseFlinkJob baseFlinkJob, RegionSegment regionSegment) {
    this.baseFlinkJob = baseFlinkJob;
    this.regionSegment = regionSegment;
    getAwsSecret =
        () -> AwsSecretsManagerClient.getSecretsValue(regionSegment.region, contentApiSecretName);
  }

  @Override
  public void validateArgs() {
    // Most validation done in getJoinOtherContentIdsFunction().
    // if contentApiRootUrl is set, then contentIdFieldKeys need to be not null.
    if (enableDummyContentApiLoader) {
      overrideTinyInsertionOtherContentIds = new NoContentDataStreamLookup();
      overrideTinyActionOtherContentIds = new NoContentDataStreamLookup();
      LOGGER.info("Setup NoContentLookup");
      return;
    }
    if (!contentApiRootUrl.isEmpty() && contentQueryClient == null) {
      Preconditions.checkArgument(
          enableInsertionContentLookup || enableActionContentLookup,
          "If --contentApiRootUrl is specified, then at least one of "
              + "--enableInsertionContentLookup or --enableActionContentLookup should be specified");

      Preconditions.checkArgument(
          !contentApiRootUrl.isEmpty(), "--contentApiRootUrl needs to be set");
      contentQueryClient =
          new ContentQueryClientImpl(contentApiRootUrl, getApiKey(), contentApiTimeout);
      LOGGER.info("Setup ContentApiLoader");
    } else {
      LOGGER.info("Did not setup ContentApiLoader.");
    }
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.of();
  }

  public boolean shouldJoinInsertionOtherContentIds() {
    return overrideTinyInsertionOtherContentIds != null || enableInsertionContentLookup;
  }

  public boolean shouldJoinActionOtherContentIds() {
    return overrideTinyActionOtherContentIds != null || enableActionContentLookup;
  }

  /** Should be used by the insertion content join code. */
  public SingleOutputStreamOperator<TinyInsertion> joinOtherInsertionContentIdsFromContentService(
      DataStream<TinyInsertion> in) {
    return AsyncDataStream.unorderedWaitWithRetry(
        in,
        getTinyInsertionOtherContentIdsFunction(),
        contentLookupOperatorTimeout.toMillis(),
        TimeUnit.MILLISECONDS,
        contentApiCapacity,
        getAsyncRetryStrategy());
  }

  /** Should be used by the action content join code. */
  public SingleOutputStreamOperator<TinyAction> joinOtherActionContentIdsFromContentService(
      DataStream<TinyAction> in) {
    return AsyncDataStream.unorderedWaitWithRetry(
        in,
        getJoinActionOtherContentIdsFunction(),
        contentLookupOperatorTimeout.toMillis(),
        TimeUnit.MILLISECONDS,
        contentApiCapacity,
        getAsyncRetryStrategy());
  }

  private <T> AsyncRetryStrategy<T> getAsyncRetryStrategy() {
    return new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<T>(
            contentLookupMaxAttempts, contentLookupFixedDelay.toMillis())
        // We always output from the function so retry if empty.
        // .ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
        .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
        .build();
  }

  /**
   * Returns a new CachingContentLookup so we can be sure we'll have separate caches for each
   * content type.
   */
  AsyncFunction<TinyInsertion, TinyInsertion> getTinyInsertionOtherContentIdsFunction() {
    if (overrideTinyInsertionOtherContentIds != null) {
      return overrideTinyInsertionOtherContentIds;
    }
    DebugIds debugIds = baseFlinkJob.getDebugIds();
    return new CachedContentLookupFunction<>(
        getContentCacheSupplier(),
        TinyInsertion::toBuilder,
        TinyInsertion.Builder::build,
        insertion -> insertion.getCore().getContentId(),
        (builder, key) -> builder.getCore().containsOtherContentIds(key),
        (builder, entry) ->
            builder.getCoreBuilder().putOtherContentIds(entry.getKey(), entry.getValue()),
        debugIds::matches);
  }

  /**
   * Returns a new CachingContentLookup so we can be sure we'll have separate caches for each
   * content type.
   */
  AsyncFunction<TinyAction, TinyAction> getJoinActionOtherContentIdsFunction() {
    if (overrideTinyActionOtherContentIds != null) {
      return overrideTinyActionOtherContentIds;
    }
    DebugIds debugIds = baseFlinkJob.getDebugIds();
    return new CachedContentLookupFunction<>(
        getContentCacheSupplier(),
        TinyAction::toBuilder,
        TinyAction.Builder::build,
        TinyAction::getContentId,
        TinyAction.Builder::containsOtherContentIds,
        (builder, entry) -> builder.putOtherContentIds(entry.getKey(), entry.getValue()),
        debugIds::matches);
  }

  ContentCacheSupplier getContentCacheSupplier() {
    if (contentCacheSupplier == null) {
      contentCacheSupplier =
          new ContentCacheSupplier(
              contentCacheMaxSize,
              contentCacheMaxBatchSize,
              contentCacheMaxBatchDelay,
              contentCacheExpiration,
              contentQueryClient,
              contentIdFieldKeys);
    }
    return contentCacheSupplier;
  }

  @VisibleForTesting
  String getApiKey() {
    String apiKey;
    if (contentApiKey.isEmpty() && !contentApiSecretName.isEmpty()) {
      if (secretStoreApiKey == null) {
        Preconditions.checkArgument(
            !regionSegment.region.isEmpty(), "--region needs to be specified");
        String secretsJson = getAwsSecret.get();
        ObjectMapper mapper = new ObjectMapper();
        try {
          Map<String, Object> jsonMap = mapper.readValue(secretsJson, Map.class);
          secretStoreApiKey = (String) jsonMap.getOrDefault(contentApiSecretKey, "");
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }
      apiKey = secretStoreApiKey;
    } else {
      apiKey = contentApiKey;
    }
    Preconditions.checkArgument(
        !apiKey.isEmpty(),
        "Content API key needs to be set.  Either through --contentApiSecretName or --secretStoreApiKey");
    return apiKey;
  }
}
