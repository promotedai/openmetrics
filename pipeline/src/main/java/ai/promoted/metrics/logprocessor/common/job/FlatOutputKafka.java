package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.JoinedEvent;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.List;

import static ai.promoted.metrics.logprocessor.common.util.StringUtil.firstNotEmpty;

public class FlatOutputKafka implements FlinkSegment {
    private final KafkaSegment kafkaSegment;

    @Option(names = {"--joinedEventTopic"}, defaultValue = "",
            description = "Overrides the JoinedEvent Kafka topic.")
    public String joinedEventTopicOverride = "";

    @Option(names = {"--joinedUserEventTopic"}, defaultValue = "",
            description = "Overrides the JoinedUserEvent Kafka topic.")
    public String joinedUserEventTopicOverride = "";

    @Option(names = {"--flatResponseInsertionTopic"}, defaultValue = "",
            description = "Overrides the FlatResponseInsertion Kafka topic.")
    public String flatResponseInsertionTopicOverride = "";

    @Option(names = {"--flatUserResponseInsertionTopic"}, defaultValue = "",
            description = "Overrides the FlatUserResponseInsertion Kafka topic.")
    public String flatUserResponseInsertionTopicOverride = "";

    @Option(names = {"--maxPollJoinedEvent"}, defaultValue = "", description = "Max number of JoinedEvent to poll in " +
            "a single Kafka call.  Defaults to --maxPollRecords")
    public String maxPollJoinedEvent = "";

    @Option(names = {"--maxPollFlatResponseInsertion"}, defaultValue = "", description = "Max number of " +
            "ResponseInsertion to poll in a single Kafka call.  Defaults to --maxPollRecords")
    public String maxPollFlatResponseInsertion = "";

    @Option(names = {"--maxJoinedEventOutOfOrderness"}, defaultValue = "PT1S", description = "The maxOutOfOrderness " +
            "for the JoinedEvent kafka. Default=PT1S")
    public Duration maxJoinedEventOutOfOrderness = Duration.parse("PT1S");

    @Option(names = {"--maxFlatResponseInsertionOutOfOrderness"}, defaultValue = "PT1S", description = "The " +
            "maxOutOfOrderness for the FlatResponseInsertion kafka. Default=PT1S")
    public Duration maxFlatResponseInsertionOutOfOrderness = Duration.parse("PT1S");

    public FlatOutputKafka(KafkaSegment kafkaSegment) {
        this.kafkaSegment = kafkaSegment;
    }

    @Override
    public void validateArgs() {
        // Do nothing.
    }


    @Override
    public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
        return ImmutableList.of(JoinedEvent.class, FlatResponseInsertion.class);
    }

    /**
     * Returns the source for JoinedEvent topics.
     */
    public SingleOutputStreamOperator<JoinedEvent> getJoinedEventSource(
            StreamExecutionEnvironment env, String consumerGroupId, String topic) {
        // NOTE: the workaround to use addSource doesn't allow max.poll.records overrides for now which is ok.
        return kafkaSegment.addProtobufSource(
                env,
                topic,
                consumerGroupId,
                JoinedEvent.class,
                JoinedEvent::parseFrom,
                maxJoinedEventOutOfOrderness,
                FlatUtil::getLogTimestamp);
    }

    /**
     * Returns the source for FlatResponseInsertion topics.
     */
    protected SingleOutputStreamOperator<FlatResponseInsertion> getFlatResponseInsertionSource(
            StreamExecutionEnvironment env, String consumerGroupId, String topic) {
        // NOTE: the workaround to use addSource doesn't allow max.poll.records overrides for now which is ok.
        return kafkaSegment.addProtobufSource(
                env,
                topic,
                consumerGroupId,
                FlatResponseInsertion.class,
                FlatResponseInsertion::parseFrom,
                maxFlatResponseInsertionOutOfOrderness,
                // This is set in FlatUtil.createFlatResponseInsertion.
                flat -> flat.getTiming().getLogTimestamp());
    }

    public DataStreamSink<JoinedEvent> addJoinedEventSink(DataStream<JoinedEvent> stream, String topic) {
        return kafkaSegment.sinkTo(
                stream,
                topic,
                kafkaSegment.getProtoKafkaSink(topic, f -> f.getIds().getPlatformId(), f -> f.getIds().getLogUserId()));
    }

    public DataStreamSink<FlatResponseInsertion> addFlatResponseInsertionSink(
            DataStream<FlatResponseInsertion> stream, String topic) {
        return kafkaSegment.sinkTo(
                stream,
                topic,
                kafkaSegment.getProtoKafkaSink(topic, f -> f.getIds().getPlatformId(), f -> f.getIds().getLogUserId()));
    }

    public String getJoinedEventTopic(String label) {
        return firstNotEmpty(joinedEventTopicOverride, kafkaSegment.getKafkaTopic(
                Constants.METRICS_TOPIC_MESSAGE_GROUP,
                kafkaSegment.formatDatasets(label, kafkaSegment.kafkaDataset),
                Constants.JOINED_EVENT_TOPIC_DATA_NAME));
    }

    public String getJoinedUserEventTopic(String label) {
        return firstNotEmpty(joinedUserEventTopicOverride, kafkaSegment.getKafkaTopic(
                Constants.METRICS_TOPIC_MESSAGE_GROUP,
                kafkaSegment.formatDatasets(label, kafkaSegment.kafkaDataset),
                Constants.JOINED_USER_EVENT_TOPIC_DATA_NAME));
    }

    public String getFlatResponseInsertionTopic(String label) {
        return firstNotEmpty(flatResponseInsertionTopicOverride, kafkaSegment.getKafkaTopic(
                Constants.METRICS_TOPIC_MESSAGE_GROUP,
                kafkaSegment.formatDatasets(label, kafkaSegment.kafkaDataset),
                Constants.FLAT_RESPONSE_INSERTION_TOPIC_DATA_NAME));
    }

    public String getFlatUserResponseInsertionTopic(String label) {
        return firstNotEmpty(flatUserResponseInsertionTopicOverride, kafkaSegment.getKafkaTopic(
                Constants.METRICS_TOPIC_MESSAGE_GROUP,
                kafkaSegment.formatDatasets(label, kafkaSegment.kafkaDataset),
                Constants.FLAT_USER_RESPONSE_INSERTION_TOPIC_FMT_DATA_NAME));
    }
}
