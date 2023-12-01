package ai.promoted.metrics.logprocessor.job.fixschema;

import ai.promoted.metrics.datafix.TestRecord1;
import ai.promoted.metrics.datafix.TestRecord2;
import ai.promoted.metrics.logprocessor.common.functions.DateHourBucketAssigner;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.FeatureFlag;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.S3Segment;
import ai.promoted.metrics.logprocessor.common.s3.S3Path;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.JoinedImpression;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.formats.avro.AvroBuilder;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.formats.avro.AvroWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

@CommandLine.Command(
    name = "fixschema",
    mixinStandardHelpOptions = true,
    version = "fixschema 1.0.0",
    description = "Creates a Flink job that fixes Avro schemas.")
public class FixSchemaJob extends BaseFlinkJob {
  private static final Logger LOGGER = LogManager.getLogger(FixSchemaJob.class);

  @CommandLine.Mixin public final S3Segment s3 = new S3Segment(this);

  @FeatureFlag
  @CommandLine.Option(
      names = {"--prefix"},
      description = "The object prefix to copy.")
  public String prefix = "";

  @FeatureFlag
  @CommandLine.Option(
      names = {"--start", "--startdt", "--startDt"},
      description = "The inclusive start date in ISO 8601.")
  public String startDt = "";

  @FeatureFlag
  @CommandLine.Option(
      names = {"--end", "--enddt", "--endDt"},
      description = "The inclusive end date in ISO 8601.")
  public String endDt = "";

  @FeatureFlag
  @CommandLine.Option(
      names = {"--outputPrefix"},
      description = "The object prefix to copy.")
  public String outputPrefix = "";

  @FeatureFlag
  @CommandLine.Option(
      names = {"--schema"},
      description = "The schema (e.g. Proto message type).")
  public String schema = "";

  public static void main(String[] args) {
    executeMain(new FixSchemaJob(), args);
  }

  // TODO - change from stream job to batch job.

  private static Schema getSchema(String schemaProto) {
    switch (schemaProto) {
      case "AttributedAction":
        return ai.promoted.metrics.logprocessor.common.avro.PromotedProtobufData.get()
            .getSchema(AttributedAction.class);
      case "DeliveryLog":
        return ai.promoted.metrics.logprocessor.common.avro.PromotedProtobufData.get()
            .getSchema(DeliveryLog.class);
      case "FlatResponseInsertion":
        return ai.promoted.metrics.logprocessor.common.avro.PromotedProtobufData.get()
            .getSchema(FlatResponseInsertion.class);
      case "JoinedImpression":
        return ai.promoted.metrics.logprocessor.common.avro.PromotedProtobufData.get()
            .getSchema(JoinedImpression.class);
      case "TestRecord1":
        // For testing since it has a smaller schema.
        return TestRecord1.SCHEMA$;
      case "TestRecord2":
        // For testing since it has a smaller schema.
        return TestRecord2.SCHEMA$;
      default:
        throw new UnsupportedOperationException("schemaProto=" + schemaProto);
    }
  }

  @Override
  public Set<FlinkSegment> getInnerFlinkSegments() {
    return ImmutableSet.of(s3);
  }

  @Override
  public void validateArgs() {
    super.validateArgs();
    Preconditions.checkArgument(!prefix.isBlank(), "--prefix must be specified.");
    Preconditions.checkArgument(!startDt.isBlank(), "--startdt must be specified.");
    Preconditions.checkArgument(!endDt.isBlank(), "--enddt must be specified.");
    Preconditions.checkArgument(!outputPrefix.isBlank(), "--outputPrefix must be specified.");
    Preconditions.checkArgument(!schema.isBlank(), "--schemaProto must be specified.");
  }

  @Override
  protected String getDefaultBaseJobName() {
    return "fix-schema";
  }

  @Override
  protected void startJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    configureExecutionEnvironment(env, parallelism, maxParallelism);
    env.setRestartStrategy(
        RestartStrategies.fixedDelayRestart(
            30, // Arbitrary to retry on failure.
            Time.of(30, TimeUnit.SECONDS)));

    fixSchema(env);
    LOGGER.info("FixSchemaJob.executionPlan\n{}", env.getExecutionPlan());
    env.execute(getJobName());
  }

  void fixSchema(StreamExecutionEnvironment env) throws IOException {
    String inputS3Path = s3.getOutputDir(prefix).build().toString();
    LOGGER.info("inputS3Path={}", inputS3Path);

    AvroInputFormat avroInputFormat =
        new AvroInputFormat<GenericRecord>(
            new org.apache.flink.core.fs.Path(inputS3Path), GenericRecord.class);
    avroInputFormat.setNestedFileEnumeration(true);

    // PR - readFile is deprecated.  I tried using FileSource but didn't see an easy way to input
    // Avro file records.  It's more code.
    DataStream<GenericRecord> records =
        add(
            env.readFile(
                avroInputFormat,
                inputS3Path,
                FileProcessingMode.PROCESS_ONCE,
                100,
                new DateFilePathFilter(startDt, endDt)),
            "source");

    Schema newSchema = getSchema(schema);

    AvroWriterFactory<GenericRecord> factory = createAvroWriterFactory(newSchema);
    S3Path outputS3Path = s3.getOutputDir(outputPrefix).build();
    add(
        records.sinkTo(
            FileSink.forBulkFormat(
                    new org.apache.flink.core.fs.Path(outputS3Path.toString()), factory)
                .withBucketAssigner(
                    new DateHourBucketAssigner<GenericRecord>(getTimestampExtractor()))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build()),
        "sink");
  }

  private AvroWriterFactory<GenericRecord> createAvroWriterFactory(final Schema newSchema)
      throws IOException {
    final List<Tuple2<String, String>> conversions = getConversions();
    AvroWriterFactory<GenericRecord> factory =
        new AvroWriterFactory<>(
            (AvroBuilder<GenericRecord> & Serializable)
                out -> {
                  ConversionGenericData data = new ConversionGenericData(conversions);
                  DatumWriter<GenericRecord> datumWriter =
                      new GenericDatumWriter<>(newSchema, data);
                  DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
                  dataFileWriter.setCodec(CodecFactory.snappyCodec());
                  dataFileWriter.create(newSchema, out);
                  return dataFileWriter;
                });
    return factory;
  }

  private SerializableToLongFunction<GenericRecord> getTimestampExtractor() {
    switch (schema) {
      case "DeliveryLog":
        return (value) -> {
          Object request = value.get("request");
          if (request == null) {
            return 0L;
          }
          Object timing = ((GenericRecord) request).get("timing");
          if (timing == null) {
            return 0L;
          }
          return (long) ((GenericRecord) timing).get("event_api_timestamp");
        };
      case "FlatResponseInsertion":
      case "JoinedEvent":
        return (value) -> {
          Object timing = value.get("timing");
          if (timing == null) {
            return 0L;
          }
          return (long) ((GenericRecord) timing).get("event_api_timestamp");
        };
      case "TestRecord1":
      case "TestRecord2":
        // For testing since it has a smaller schema.
        return (value) -> (long) value.get("eventApiTimestamp");
      default:
        throw new UnsupportedOperationException("schemaProto=" + schema);
    }
  }

  private List<Tuple2<String, String>> getConversions() {
    // So we can serialize the list.
    ArrayList<Tuple2<String, String>> conversions = new ArrayList<>();
    switch (schema) {
      case "DeliveryLog":
      case "FlatResponseInsertion":
      case "JoinedEvent":
        conversions.add(
            new Tuple2<>("com.google.protobuf.ListValue", "com.google.protobuf.ListValue"));
        conversions.add(new Tuple2<>("com.google.protobuf.Value", "com.google.protobuf.Value"));
        conversions.add(
            new Tuple2<>(
                "ai.promoted.proto.delivery.Request", "ai.promoted.proto.delivery.FlatRequest"));
        return conversions;
      case "TestRecord1":
      case "TestRecord2":
        conversions.add(
            new Tuple2<>(
                "ai.promoted.metrics.datafix.RecursiveValue", "ai.promoted.metrics.datafix.Value"));
        return conversions;
      default:
        throw new UnsupportedOperationException("schemaProto=" + schema);
    }
  }

  private final class FixSchema implements MapFunction<GenericRecord, GenericRecord> {
    private final SerializableSupplier<Schema> newSchemaSupplier;
    private final List<Tuple2<String, String>> oldToNewRecordPrefixes;
    private transient Schema newSchema;
    private transient ConversionGenericData conversionGenericData;

    FixSchema(
        SerializableSupplier<Schema> newSchemaSupplier,
        List<Tuple2<String, String>> oldToNewRecordPrefixes) {
      this.newSchemaSupplier = newSchemaSupplier;
      this.oldToNewRecordPrefixes = oldToNewRecordPrefixes;
    }

    @Override
    public GenericRecord map(GenericRecord record) {
      if (newSchema == null) {
        newSchema = newSchemaSupplier.get();
      }
      return getFixedGenericData().deepCopy(newSchema, record);
    }

    private ConversionGenericData getFixedGenericData() {
      if (conversionGenericData == null) {
        conversionGenericData = new ConversionGenericData(oldToNewRecordPrefixes);
      }
      return conversionGenericData;
    }
  }
}
