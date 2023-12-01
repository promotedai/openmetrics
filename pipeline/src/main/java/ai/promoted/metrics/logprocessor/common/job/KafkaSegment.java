package ai.promoted.metrics.logprocessor.common.job;

import static ai.promoted.metrics.logprocessor.common.util.StringUtil.isBlank;

import ai.promoted.metrics.logprocessor.common.constant.Constants;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Set;
import java.util.stream.Collectors;
import picocli.CommandLine;
import picocli.CommandLine.Option;

// TODO: https://github.com/remkop/picocli/issues/1527
public class KafkaSegment implements FlinkSegment {
  @CommandLine.Option(
      names = {"--kafkaDataset"},
      defaultValue = Constants.DEFAULT_KAFKA_DATASET,
      description = "The middle part of the Kafka topic name. Default=default")
  public String kafkaDataset = Constants.DEFAULT_KAFKA_DATASET;

  @Option(
      names = {"-b", "--bootstrap.servers"},
      defaultValue = Constants.DEFAULT_BOOTSTRAP_SERVERS,
      description = "Kafka bootstrap servers.")
  public String bootstrapServers = Constants.DEFAULT_BOOTSTRAP_SERVERS;

  // Prod value = "SSL"
  @Option(
      names = {"--kafkaSecurityProtocol"},
      defaultValue = "",
      description = "Kafka Source Security Protocol.")
  public String kafkaSecurityProtocol = "";

  @Override
  public void validateArgs() {}

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.of();
  }

  /**
   * Returns a Kafka topic name from the given topic format and dataset specification(s). Generally,
   * if this is for an input source, don't set any specs to get the default/production topic. For
   * output sinks, you should set something for non-production job runs.
   *
   * <p>Kafka topic naming convention - see the comment in Constants.
   *
   * @param messageGroup first part of the kafka topic name.
   * @param datasets the middle part of the kafka topic.
   * @param dataName the last part of the kafka topic name.
   */
  public String getKafkaTopic(String messageGroup, String datasets, String dataName) {
    return messageGroup + "." + datasets + "." + dataName;
  }

  public String getDataset(String label) {
    return ImmutableList.of(label, kafkaDataset).stream()
        .filter(s -> !isBlank(s))
        .collect(Collectors.joining("."));
  }
}
