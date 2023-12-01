package ai.promoted.metrics.logprocessor.fakedatagenerator;

import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestIterator;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestIteratorOptions;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentType;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.FakeInsertionSurface;
import ai.promoted.metrics.logprocessor.common.records.ProtoKafkaSerializationSchema;
import ai.promoted.proto.event.LogRequest;
import com.google.protobuf.Descriptors;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "fakeloggenerator",
    mixinStandardHelpOptions = true,
    version = "fakeloggenerator 1.0.0",
    description = "A generator which pushes fake LogRequests into a Kafka topic.")
public class LogGenerator implements Callable<Integer> {
  private static final Logger LOGGER = LogManager.getLogger(LogGenerator.class);

  @Option(
      names = {"--metricsApiKafkaDataset"},
      defaultValue = Constants.DEPRECATED_DEFAULT_KAFKA_DATASET,
      description = "The middle part of the Kafka topic name. Default=event")
  private String metricsApiKafkaDataset = Constants.DEPRECATED_DEFAULT_KAFKA_DATASET;

  @Option(
      names = {"--logRequestTopic"},
      defaultValue = "",
      description = "Overrides the input LogRequest Kafka topic.")
  private String logRequestTopicOverride = "";

  @Option(
      names = {"--startEventApiTimestamp"},
      defaultValue = "0",
      description =
          "If set, will write out event_api_timestamps based on startTime.  Defaults to when the LogGenerator is started.")
  private long startEventApiTimestamp = 0L;

  @Option(
      names = {"-u", "--users"},
      defaultValue = "25",
      description = "Number of users to generate.")
  private int users = 25;

  @Option(
      names = {"-s", "--sessionsPerUser"},
      defaultValue = "1",
      description = "Number of sessions per user.")
  private int sessionsPerUser = 1;

  @Option(
      names = {"-v", "--viewsPerSession"},
      defaultValue = "100",
      description = "Number of views per session.")
  private int viewsPerSession = 100;

  @Option(
      names = {"--delayBetweenViewInSeconds"},
      defaultValue = "1",
      description = "How long to wait between creating new views for the same session.")
  private int delayBetweenViewInSeconds = 1;

  @Option(
      names = {"-r", "--requestsPerViews"},
      defaultValue = "1",
      description = "Number of requests per views.")
  private int requestsPerViews = 1;

  @Option(
      names = {"-a", "--autoViewsPerSession"},
      defaultValue = "100",
      description = "Number of auto views per session.")
  private int autoViewsPerSession = 100;

  @Option(
      names = {"--delayBetweenAutoViewInSeconds"},
      defaultValue = "1",
      description = "How long to wait between creating new auto views for the same session.")
  private int delayBetweenAutoViewInSeconds = 1;

  @Option(
      names = {"-i", "--responseInsertionsPerRequest"},
      defaultValue = "20",
      description = "Number of response insertions per request.")
  private int responseInsertionsPerRequest = 20;

  // TODO - support fraction for this.
  @Option(
      names = {"--insertionImpressedRate"},
      defaultValue = "0.7",
      description = ".")
  private float insertionImpressedRate = 0.7f;

  @Option(
      names = {"--impressionNavigateRate"},
      defaultValue = "0.2",
      description = "Number of navigates per impression.")
  private float impressionNavigateRate = 0.2f;

  @Option(
      names = {"--navigateAddToCartRate"},
      defaultValue = "0.2",
      description = "Number of `add to carts` per navigate.")
  private float navigateAddToCartRate = 0.2f;

  @Option(
      names = {"--navigateCheckoutRate"},
      defaultValue = "0.2",
      description = "Number of checkout per navigate.")
  private float navigateCheckoutRate = 0.2f;

  @Option(
      names = {"--checkoutPurchaseRate"},
      defaultValue = "0.3",
      description = "Number of purchases per checkout.")
  private float checkoutPurchaseRate = 0.3f;

  @Option(
      names = {"--delayMultiplier"},
      defaultValue = "1.0",
      description =
          "Applies the specified delayMultiplier to the various event delays.  The effect can be compounded with --uniformRandomDelay.")
  private float delayMultiplier = 1.0f;

  @Option(
      names = {"--uniformRandomDelay"},
      negatable = true,
      description =
          "Applies a random delay multiplier from a uniform distribution between [0, 1) to the various event delays.  The input delay is the various event delays and the --delayMultiplier if specified.")
  private boolean uniformRandomDelay = false;

  @Option(
      names = {"--useInsertionMatrixFormat"},
      negatable = true,
      description = "If true, use the insertion matrix format.")
  private boolean useInsertionMatrixFormat = true;

  @Option(
      names = {"--no-setupForInferredIds"},
      negatable = true,
      description = "If true, omit some ids used for joining to test inferred references.")
  private boolean setupForInferredIds = true;

  @Option(
      names = {"--no-writeUsers"},
      negatable = true,
      description = "If true, write User records. Defaults to true.")
  private boolean writeUsers = true;

  @Option(
      names = {"--missingViewRate"},
      defaultValue = "0.0",
      description = "The rate of us dropping Views.")
  private float missingViewRate = 0.0f;

  @Option(
      names = {"--missingAutoViewRate"},
      defaultValue = "0.0",
      description = "The rate of us dropping AutoViews.")
  private float missingAutoViewRate = 0.0f;

  @Option(
      names = {"--missingDeliveryLogRate"},
      defaultValue = "0.0",
      description = "The rate of us dropping DeliveryLog.")
  private float missingDeliveryLogRate = 0.0f;

  @Option(
      names = {"--missingImpressionRate"},
      defaultValue = "0.0",
      description = "The rate of us dropping Impressions.")
  private float missingImpressionRate = 0.0f;

  @Option(
      names = {"--miniSdkRate"},
      defaultValue = "0.0",
      description = "Percent of DeliveryLogs that should be generated from the SDK.")
  private float miniSdkRate = 0.0f;

  @Option(
      names = {"--shadowTrafficRate"},
      defaultValue = "0.0",
      description = "Percent of mini SDK DeliveryLogs that also have shadow traffic logs.")
  private float shadowTrafficRate = 0.0f;

  @Option(
      names = {"--redundantImpressionRate"},
      defaultValue = "0.0",
      description = "Rate of deliverylogs that produce redundant impressions.")
  private float redundantImpressionRate = 5;

  @Option(
      names = {"--maxRedundantImpressionsPerDeliveryLog"},
      defaultValue = "5",
      description = "Max number of impressions when a DeliveryLog has redundant impressions")
  private int maxRedundantImpressionsPerDeliveryLog = 5;

  @Option(
      names = {"--emptyLogUserIdRate"},
      defaultValue = "0.0",
      description = "Rate of logUserIds that are empty strings.")
  private float emptyLogUserIdRate = 0.0f;

  @Option(
      names = {"--kafkaCompressionType"},
      defaultValue = "snappy",
      description =
          "The Kafka producer `compression.type` value.  Examples: none, gzip, snapper, lz4 or zstd.")
  private String kafkaCompressionType = "snappy";

  @Option(
      names = {"--requestContentType0"},
      defaultValue = "ITEM",
      description = "The type of content for the first request page.")
  private ContentType requestContentType0 = ContentType.ITEM;

  @Option(
      names = {"--requestContentType1"},
      description = "The type of content for the request page.")
  private Optional<ContentType> requestContentType1 = Optional.empty();

  @Option(
      names = {"-b", "--bootstrap.servers"},
      defaultValue = Constants.DEFAULT_BOOTSTRAP_SERVERS,
      description = "Kafka bootstrap servers.")
  private String bootstrapServers = Constants.DEFAULT_BOOTSTRAP_SERVERS;

  public static void main(String[] args) throws Exception {
    int exitCode = new CommandLine(new LogGenerator()).execute(args);
    System.exit(exitCode);
  }

  private String firstNonEmpty(String first, String second) {
    if (!first.isEmpty()) {
      return first;
    }
    return second;
  }

  private String getLogRequestTopic() {
    return firstNonEmpty(
        logRequestTopicOverride,
        Constants.LOG_REQUEST_TOPIC_MESSAGE_GROUP
            + "."
            + metricsApiKafkaDataset
            + "."
            + Constants.LOG_REQUEST_TOPIC_DATA_NAME);
  }

  @Override
  public Integer call() throws Exception {
    Properties kafkaProps = createKafkaProperties();
    KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps);

    LogRequestIterator logIterator = createLogRequestIterator();
    String topic = getLogRequestTopic();
    LOGGER.info("Writing to topic={}", topic);
    ProtoKafkaSerializationSchema<LogRequest> logRequestSerializer =
        new ProtoKafkaSerializationSchema<>(
            topic,
            LogRequest::getPlatformId,
            (logRequest) -> logRequest.getUserInfo().getLogUserId());

    TrackExceptionsCallback callback = new TrackExceptionsCallback();
    long logRequestCount = 0;

    Map<Descriptors.FieldDescriptor, Long> descriptorToCount = new HashMap();
    long lastFlush = 0;
    long lastLog = 0;
    while (logIterator.hasNext()) {
      Instant nextTime = logIterator.peekTime();
      Instant now = Instant.now();
      if (now.isBefore(nextTime)) {
        long sleepMillis = nextTime.toEpochMilli() - now.toEpochMilli();
        LOGGER.info("sleepMillis={}", sleepMillis);
        Thread.sleep(sleepMillis);
      }

      LogRequest current = logIterator.next();
      producer.send(logRequestSerializer.serialize(current), callback);
      logRequestCount++;

      current.getAllFields().entrySet().stream()
          .filter(entry -> entry.getKey().isRepeated())
          .forEach(
              entry -> {
                long count = ((List) entry.getValue()).size();
                descriptorToCount.put(
                    entry.getKey(), descriptorToCount.getOrDefault(entry.getKey(), 0L) + count);
              });

      if (logRequestCount >= lastFlush + 10) {
        producer.flush();
        callback.throwIfHasException();
        lastFlush = logRequestCount;
        if (logRequestCount >= lastLog + 40000) {
          LOGGER.info("Sent {} LogRequests", logRequestCount);
          lastLog = logRequestCount;
        }
      }
    }
    producer.flush();
    callback.throwIfHasException();

    long subrecordTotal =
        descriptorToCount.values().stream().mapToLong(value -> value.longValue()).sum();
    Map<String, Long> readableDescriptorToCount =
        descriptorToCount.entrySet().stream()
            .collect(Collectors.toMap(key2 -> key2.getKey().getName(), Map.Entry::getValue));

    LOGGER.info("Sent {} total LogRequests", logRequestCount);
    LOGGER.info("Sent {} total subrecords", subrecordTotal);
    LOGGER.info("Sent subrecords of each type: {}", readableDescriptorToCount);
    LOGGER.info("Done");
    return 0;
  }

  private Properties createKafkaProperties() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    kafkaProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    kafkaProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaCompressionType);
    return kafkaProps;
  }

  private LogRequestIterator createLogRequestIterator() {
    final Random random = new Random();
    Supplier<Instant> nowSupplier = () -> Instant.now();
    Supplier<Instant> eventApiTimestampSupplier;
    if (startEventApiTimestamp == 0L) {
      eventApiTimestampSupplier = nowSupplier;
    } else {
      final Instant nowStart = Instant.now();
      // Offset the eventApiTimestamp by how much time has passed in the LogGenerator run.
      eventApiTimestampSupplier =
          () ->
              Instant.ofEpochMilli(
                  startEventApiTimestamp + Instant.now().toEpochMilli() - nowStart.toEpochMilli());
    }
    return new LogRequestIterator(
        LogRequestFactory.setTestMarketplaceFields(LogRequestIteratorOptions.builder())
            .setUsers(users)
            .setWriteUserId(true)
            .setSessionsPerUser(sessionsPerUser)
            .setViewsPerSession(viewsPerSession)
            .setAutoViewsPerSession(autoViewsPerSession)
            .setDelayBetweenViewInSeconds(delayBetweenViewInSeconds)
            .setRequestsPerViews(requestsPerViews)
            .setRequestInsertionSurface0(
                FakeInsertionSurface.create(requestContentType0, true, true))
            .setRequestInsertionSurface1(
                requestContentType1.isPresent()
                    ? Optional.of(
                        FakeInsertionSurface.create(requestContentType1.get(), true, true))
                    : Optional.empty())
            .setDelayBetweenAutoViewInSeconds(delayBetweenAutoViewInSeconds)
            .setResponseInsertionsPerRequest(responseInsertionsPerRequest)
            .setInsertionImpressedRate(insertionImpressedRate)
            .setImpressionNavigateRate(impressionNavigateRate)
            .setNavigateCheckoutRate(navigateCheckoutRate)
            .setNavigateAddToCartRate(navigateAddToCartRate)
            .setCheckoutPurchaseRate(checkoutPurchaseRate)
            // When set via flags, allow one or the other.
            .setInsertionMatrixFormat(useInsertionMatrixFormat)
            .setInsertionFullFormat(!useInsertionMatrixFormat)
            .setSetupForInferredIds(setupForInferredIds)
            .setNowSupplier(nowSupplier)
            .setEventApiTimestampSupplier(eventApiTimestampSupplier)
            .setWriteUsers(writeUsers)
            .setMissingViewRate(missingViewRate)
            .setMissingAutoViewRate(missingAutoViewRate)
            .setMissingDeliveryLogRate(missingDeliveryLogRate)
            .setMissingImpressionRate(missingImpressionRate)
            .setMiniSdkRate(miniSdkRate)
            .setShadowTrafficRate(shadowTrafficRate)
            .setRedundantImpressionRate(redundantImpressionRate)
            .setMaxRedundantImpressionsPerDeliveryLog(maxRedundantImpressionsPerDeliveryLog)
            .setUserUuidSupplier(() -> "userId-" + UUID.randomUUID())
            .setAnonUserUuidSupplier(
                (userId) -> {
                  if (LogRequestIterator.matchesRateOption(
                      emptyLogUserIdRate, userId, "setLogUserUuidSupplier")) {
                    return "";
                  } else {
                    return UUID.randomUUID().toString();
                  }
                })
            .setCohortMembershipUuidSupplier(() -> UUID.randomUUID().toString())
            .setSessionUuidSupplier(() -> UUID.randomUUID().toString())
            .setViewUuidSupplier(() -> UUID.randomUUID().toString())
            .setAutoViewUuidSupplier(() -> UUID.randomUUID().toString())
            .setRequestUuidSupplier(() -> UUID.randomUUID().toString())
            .setResponseInsertionUuidSupplier(() -> UUID.randomUUID().toString())
            .setImpressionUuidSupplier(() -> UUID.randomUUID().toString())
            .setActionUuidSupplier(() -> UUID.randomUUID().toString())
            .setDelayMultiplier(
                (delay) -> {
                  long resultMillis = (long) (delay.toMillis() * delayMultiplier);
                  if (uniformRandomDelay) {
                    resultMillis = (long) (resultMillis * random.nextFloat());
                  }
                  return Duration.ofMillis(resultMillis);
                })
            .build());
  }

  // The Java auto-formatter incorrectly adds `final`.  This forces final to not be added.
  protected void preventFinal() {
    metricsApiKafkaDataset = Constants.DEPRECATED_DEFAULT_KAFKA_DATASET;
    logRequestTopicOverride = "";
    startEventApiTimestamp = 0L;
    users = 25;
    sessionsPerUser = 1;
    viewsPerSession = 100;
    delayBetweenViewInSeconds = 1;
    requestsPerViews = 1;
    autoViewsPerSession = 100;
    delayBetweenAutoViewInSeconds = 1;
    responseInsertionsPerRequest = 20;
    insertionImpressedRate = 0.7f;
    impressionNavigateRate = 0.2f;
    navigateAddToCartRate = 0.2f;
    navigateCheckoutRate = 0.2f;
    checkoutPurchaseRate = 0.3f;
    delayMultiplier = 1.0f;
    uniformRandomDelay = false;
    useInsertionMatrixFormat = true;
    setupForInferredIds = true;
    writeUsers = true;
    missingViewRate = 0.0f;
    missingAutoViewRate = 0.0f;
    missingDeliveryLogRate = 0.0f;
    missingImpressionRate = 0.0f;
    miniSdkRate = 0.0f;
    shadowTrafficRate = 0.0f;
    redundantImpressionRate = 0.0f;
    maxRedundantImpressionsPerDeliveryLog = 0;
    emptyLogUserIdRate = 0.0f;
    kafkaCompressionType = null;
    requestContentType0 = null;
    requestContentType1 = null;
    bootstrapServers = null;
  }
}
