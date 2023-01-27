package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.logprocessor.common.constant.Constants;
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

/**
 * The segment that offers Kafka sinks/sources for {{@link ai.promoted.metrics.logprocessor.job.raw.RawOutputJob}}.
 */
public class RawOutputKafka implements FlinkSegment {
    private final KafkaSegment kafkaSegment;

    @Option(names = {"--logUserUserTopic"}, defaultValue = "",
            description = "Overrides the LogUserUser Kafka topic.")
    public String logUserUserTopicOverride = "";

    @Option(names = {"--maxPollLogUserUser"}, defaultValue = "", description = "Max number of LogUserUser to poll in " +
            "a single Kafka call.  Defaults to --maxPollRecords")
    public String maxPollLogUserUser = "";

    @Option(names = {"--maxLogUserUserOutOfOrderness"}, defaultValue = "PT1S", description = "The maxOutOfOrderness " +
            "for the LogUserUser kafka. Default=PT1S")
    public Duration maxLogUserUserOutOfOrderness = Duration.parse("PT1S");

    public RawOutputKafka(KafkaSegment kafkaSegment) {
        this.kafkaSegment = kafkaSegment;
    }

    @Override
    public void validateArgs() {
        // Do nothing.
    }

    @Override
    public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
        return ImmutableList.of();
    }

    /**
     * Returns the source for LogUserUser topics.
     */
    public SingleOutputStreamOperator<LogUserUser> getLogUserUserSource(
            StreamExecutionEnvironment env, String consumerGroupId, String topic) {
        return kafkaSegment.addAvroSource(
                env,
                topic,
                consumerGroupId,
                LogUserUser.class,
                maxLogUserUserOutOfOrderness,
                LogUserUser::getEventApiTimestamp);
    }

    /**
     * Output the LogUserUser stream to a Kafka topic.
     */
    public DataStreamSink<LogUserUser> addLogUserUserSink(DataStream<LogUserUser> stream, String topic) {
        return kafkaSegment.sinkTo(
                stream,
                topic,
                kafkaSegment.getAvroKafkaSink(
                        topic,
                        LogUserUser::getPlatformId,
                        LogUserUser::getLogUserId,
                        LogUserUser.class));
    }

    public String getLogUserUserTopic(String label) {
        return firstNotEmpty(logUserUserTopicOverride, kafkaSegment.getKafkaTopic(
                Constants.METRICS_TOPIC_MESSAGE_GROUP,
                kafkaSegment.formatDatasets(label, kafkaSegment.kafkaDataset),
                Constants.LOG_USER_USER_EVENT_TOPIC_DATA_NAME));
    }
}
