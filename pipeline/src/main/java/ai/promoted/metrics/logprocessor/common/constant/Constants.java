package ai.promoted.metrics.logprocessor.common.constant;

/** Utility containing useful constants. */
public interface Constants {

  String LIVE_LABEL = "live";
  String DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092";

  // Kafka topic naming convention.
  // https://cnr.sh/essays/how-paint-bike-shed-kafka-topic-naming-conventions
  // Kafka topic: "<message type>.<dataset name>.<data name>"
  // Can contain multiple data set levels for labels.
  // Kafka topic: "<message type>.<label>.<dataset name>.<data name>"
  String DEFAULT_KAFKA_DATASET = "default";
  // Deprecated - Still needed because our input LogRequest topic uses it.
  String DEPRECATED_DEFAULT_KAFKA_DATASET = "event";
  String LOG_REQUEST_TOPIC_MESSAGE_GROUP = "tracking";
  String LOG_REQUEST_TOPIC_DATA_NAME = "log-request";

  // Validated inputs
  String COHORT_MEMBERSHIP_TOPIC_DATA_NAME = "cohort-membership";
  String VIEW_TOPIC_DATA_NAME = "view";
  String DELIVERY_LOG_TOPIC_DATA_NAME = "delivery-log";
  String IMPRESSION_TOPIC_DATA_NAME = "impression";
  String ACTION_TOPIC_DATA_NAME = "action";
  String DIAGNOSTICS_TOPIC_DATA_NAME = "diagnostics";

  String VALIDATION_ERROR_TOPIC_DATA_NAME = "validation-error";

  // Gets prefixed to a TOPIC_DATA_NAME;
  String INVALID_PREFIX = "invalid-";
  String INVALID_COHORT_MEMBERSHIP_TOPIC_DATA_NAME =
      INVALID_PREFIX + COHORT_MEMBERSHIP_TOPIC_DATA_NAME;
  String INVALID_VIEW_TOPIC_DATA_NAME = INVALID_PREFIX + VIEW_TOPIC_DATA_NAME;
  String INVALID_DELIVERY_LOG_TOPIC_DATA_NAME = INVALID_PREFIX + DELIVERY_LOG_TOPIC_DATA_NAME;
  String INVALID_IMPRESSION_TOPIC_DATA_NAME = INVALID_PREFIX + IMPRESSION_TOPIC_DATA_NAME;
  String INVALID_ACTION_TOPIC_DATA_NAME = INVALID_PREFIX + ACTION_TOPIC_DATA_NAME;
  String INVALID_DIAGNOSTICS_TOPIC_DATA_NAME = INVALID_PREFIX + DIAGNOSTICS_TOPIC_DATA_NAME;

  String METRICS_TOPIC_MESSAGE_GROUP = "metrics";
  String JOINED_IMPRESSION_TOPIC_DATA_NAME = "joined-impression";
  String ATTRIBUTED_ACTION_TOPIC_DATA_NAME = "attributed-action";
  String FLAT_RESPONSE_INSERTION_TOPIC_DATA_NAME = "flat-response-insertion";
  String RETAINED_USER_EVENT_TOPIC_DATA_NAME = "retained-user";
  // TODO - delete and replace with retained-user.
  String LOG_USER_USER_EVENT_TOPIC_DATA_NAME = "log-user-user-event";
  String ANON_USER_RETAINED_USER_EVENT_TOPIC_DATA_NAME = "anon-user-retained-user-event";
  String DEFAULT_S3_SCHEME = "s3a://";

  /**
   * Max length of a user session. We currently use this internally for cleaning up older session
   * state. 6 hours was chosen arbitrarily based on session lengths in other systems. The value
   * isn't very important right now. Most of our platforms are not designed to be used multiple
   * times per day. We will eventually move this to a configuration per platform.
   */
  long SESSION_DURATION_HOURS = 6;

  static String getS3BucketUri(String bucketName) {
    return Constants.DEFAULT_S3_SCHEME + bucketName + "/";
  }
}
