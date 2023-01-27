package ai.promoted.metrics.logprocessor.common.constant;

/**
 * Utility containing useful constants.
 **/
public interface Constants {

    String LIVE_LABEL = "live";
    String DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092";

    // Kafka topic naming convention.
    // https://riccomini.name/how-paint-bike-shed-kafka-topic-naming-conventions
    // Kafka topic: "<message type>.<dataset name>.<data name>"
    // Can contain multiple data set levels for labels.
    // Kafka topic: "<message type>.<label>.<dataset name>.<data name>"
    String DEFAULT_KAFKA_DATASET = "default";
    // Deprecated - Still needed because our input LogRequest topic uses it.
    String DEPRECATED_DEFAULT_KAFKA_DATASET = "event";
    String LOG_REQUEST_TOPIC_MESSAGE_GROUP = "tracking";
    String LOG_REQUEST_TOPIC_DATA_NAME = "log-request";

    String METRICS_TOPIC_MESSAGE_GROUP = "metrics";
    String JOINED_EVENT_TOPIC_DATA_NAME = "joined-event";
    String JOINED_USER_EVENT_TOPIC_DATA_NAME = "joined-user-event";
    String FLAT_RESPONSE_INSERTION_TOPIC_DATA_NAME = "flat-response-insertion";
    String LOG_USER_USER_EVENT_TOPIC_DATA_NAME = "log-user-user-event";
    String FLAT_USER_RESPONSE_INSERTION_TOPIC_FMT_DATA_NAME = "flat-user-response-insertion";

    String DEFAULT_S3_SCHEME = "s3a://";

    /**
     * Max length of a user session.
     * We currently use this internally for cleaning up older session state.
     * 6 hours was chosen arbitrarily based on session lengths in other systems.
     * The value isn't very important right now.  Most of our platforms
     * are not designed to be used multiple times per day.
     * We will eventually move this to a configuration per platform.
     */
    long SESSION_DURATION_HOURS = 6;

    static String getS3BucketUri(String bucketName) {
        return Constants.DEFAULT_S3_SCHEME + bucketName + "/";
    }
}
