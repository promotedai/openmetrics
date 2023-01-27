package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.aws.AwsSecretsManagerClient;
import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.content.common.ContentAPILoader;
import ai.promoted.metrics.logprocessor.common.functions.content.datastream.CachingContentDataStreamLookup;
import ai.promoted.metrics.logprocessor.common.functions.content.common.ContentAPILoaderImpl;
import ai.promoted.metrics.logprocessor.common.functions.content.datastream.NoContentDataStreamLookup;
import ai.promoted.proto.event.TinyEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ContentApiSegment implements FlinkSegment {
    private static final Logger LOGGER = LogManager.getLogger(ContentApiSegment.class);

    // Content Flags.
    @FeatureFlag
    @Option(names = {"--contentApiRootUrl"}, defaultValue = "",
            description = "If set, turns on content joining.  Format matches `http://ee1a-67-168-60-51.ngrok.io/` or `http://localhost:5150/`.")
    public String contentApiRootUrl = "";

    @Option(names = {"--contentApiKey"}, defaultValue = "", description = "The API Key for the Content API.  Default=empty string")
    public String contentApiKey = "";

    @Option(names = {"--contentApiTimeout"}, defaultValue = "PT1S", description = "Timeout or the Content API call. Default=PT1S.")
    public Duration contentApiTimeout = Duration.parse("PT1S");

    @Option(names = {"--region"}, defaultValue = "", description = "The AWS Region. Default=empty string.")
    public String region = "";

    @Option(names = {"--contentApiSecretName"}, defaultValue = "", description = "The AWS SecretsManager secret containing the Content API key.  Empty means do not use SecretsManager. Default=empty string.")
    public String contentApiSecretName = "";

    // TODO - future optimizations - we can split this into 3 flags to reduce the number of key checks.
    // Splitting this would make the code more complex.
    @Option(names = {"--contentIdFieldKeys"}, description = "Which fields from Content CMS and Properties should be used for other content IDs.  The order impacts the inferred reference join ordering.  Defaults to empty set.")
    public List<String> contentIdFieldKeys = new LinkedList<String>();

    @Option(names = {"--contentApiCapacity"}, defaultValue = "10", description = "Maximum capacity setting for Content API.  This probably gets operated per client instance.  Default=10.")
    public int contentApiCapacity = 10;

    @Option(names = {"--contentCacheMaxSize"}, defaultValue = "50000", description = "Maximum size of content cache.  Default=50k.")
    public int contentCacheMaxSize = 50000;

    @Option(names = {"--contentApiMaxAttempts"}, defaultValue = "3", description = "Maximum number of Content API attempts (1 + maxRetries) per content lookup.  Handles networking issuse. Default=3.")
    public int contentApiMaxAttempts = 3;

    @Option(names = {"--contentApiMaxAttemptsPerStatusCode"}, description = "Maximum number of Content API attempts (1 + maxRetries) per content lookup per http status code.  Default=No custom retries.")
    Map<Integer, Integer> contentApiMaxAttemptsPerStatusCode = ImmutableMap.of();

    @Option(names = {"--contentApiMaxAllowedConsecutiveErrors"}, defaultValue = "0", description = "Can be used to kill the overall job if there are too many consecutive Content API failures.  Default=0 (kill the job).")
    public int contentApiMaxAllowedConsecutiveErrors = 0;

    @Option(names = {"--contentCacheExpiration"}, defaultValue = "P1D", description = "Expiration time for content cache. Default=PT1D to capture global patterns.")
    public Duration contentCacheExpiration = Duration.parse("P1D");

    @Option(names = {"--enableDummyContentApiLoader"}, defaultValue = "false", description = "This can be enabled to create the Flink operator (with uids).  This will allow us to not get the drop state error when recovering from a savepoint. Default=false.")
    public boolean enableDummyContentApiLoader = false;

    RichAsyncFunction<TinyEvent, TinyEvent> overrideJoinOtherContentIds;
    public ContentAPILoader contentApiLoader;
    private String secretStoreApiKey;
    @VisibleForTesting
    Supplier<String> getAwsSecret = () -> AwsSecretsManagerClient.getSecretsValue(region, contentApiSecretName);

    private final BaseFlinkJob baseFlinkJob;

    public ContentApiSegment(BaseFlinkJob baseFlinkJob) {
        this.baseFlinkJob = baseFlinkJob;
    }

    @Override
    public void validateArgs() {
        // Most validation done in getJoinOtherContentIdsFunction().
        // if contentApiRootUrl is set, then contentIdFieldKeys need to be not null.
        if (enableDummyContentApiLoader) {
            overrideJoinOtherContentIds = new NoContentDataStreamLookup();
            LOGGER.info("Setup NoContentLookup");
            return;
        }
        if (!contentApiRootUrl.isEmpty() && contentApiLoader == null) {
            Preconditions.checkArgument(!contentApiRootUrl.isEmpty(), "--contentApiRootUrl needs to be set");
            contentApiLoader = new ContentAPILoaderImpl(
                    contentApiRootUrl,
                    getApiKey(),
                    contentApiMaxAttempts,
                    contentApiMaxAttemptsPerStatusCode,
                    contentApiMaxAllowedConsecutiveErrors);
            LOGGER.info("Setup ContentApiLoader");
        } else {
            LOGGER.info("Did not setup ContentApiLoader.");
        }
    }

    @Override
    public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
        return ImmutableList.of();
    }

    public boolean shouldJoinOtherContentIds() {
        return overrideJoinOtherContentIds != null || contentApiLoader != null;
    }

    public SingleOutputStreamOperator<TinyEvent> joinOtherContentIdsFromContentService(DataStream<TinyEvent> in) {
        return AsyncDataStream.unorderedWait(in, getJoinOtherContentIdsFunction(), contentApiTimeout.toMillis(), TimeUnit.MILLISECONDS, contentApiCapacity);
    }

    /**
     * Returns a new CachingContentLookup so we can be sure we'll have separate caches for each content type.
     */
    AsyncFunction<TinyEvent, TinyEvent> getJoinOtherContentIdsFunction() {
        if (overrideJoinOtherContentIds != null) {
            return overrideJoinOtherContentIds;
        }
        return new CachingContentDataStreamLookup(
                contentCacheMaxSize,
                contentCacheExpiration,
                contentApiLoader,
                contentIdFieldKeys,
                baseFlinkJob.getDebugIds());
    }

    @VisibleForTesting
    String getApiKey() {
        String apiKey;
        if (contentApiKey.isEmpty() && !contentApiSecretName.isEmpty()) {
            if (secretStoreApiKey == null) {
                Preconditions.checkArgument(!region.isEmpty(), "--region needs to be specified");
                String secretsJson = getAwsSecret.get();
                ObjectMapper mapper = new ObjectMapper();
                try {
                    Map<String, Object> jsonMap = mapper.readValue(secretsJson, Map.class);
                    secretStoreApiKey = (String) jsonMap.getOrDefault("api-key", "");
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
            apiKey = secretStoreApiKey;
        } else {
            apiKey = contentApiKey;
        }
        Preconditions.checkArgument(!apiKey.isEmpty(),
                "Content API key needs to be set.  Either through --contentApiSecretName or --secretStoreApiKey");
        return apiKey;
    }
}