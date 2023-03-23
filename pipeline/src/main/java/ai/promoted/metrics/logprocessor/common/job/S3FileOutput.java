package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.avro.FixedProtobufData;
import ai.promoted.metrics.logprocessor.common.avro.FixedProtobufDatumWriter;
import ai.promoted.metrics.logprocessor.common.functions.DateHourBucketAssigner;
import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.SerializableToLongFunction;
import ai.promoted.metrics.logprocessor.common.s3.S3Path;
import ai.promoted.proto.common.Timing;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroBuilder;
import org.apache.flink.formats.avro.AvroWriterFactory;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.proto.ProtoWriteSupport;
import picocli.CommandLine.Option;

public class S3FileOutput implements FlinkSegment {
  private static final Logger LOGGER = LogManager.getLogger(S3FileOutput.class);
  private final BaseFlinkJob job;
  private final S3Segment s3;

  @FeatureFlag
  @Option(
      names = {"--no-logAvro"},
      negatable = true,
      description = "Write Avro files with 'fixed' non-recursive schemas.")
  public boolean logAvro = true;

  @FeatureFlag
  @Option(
      names = {"--logParquet"},
      negatable = true,
      description = "Write Parquet files with non-recursive schemas.")
  public boolean logParquet = false;

  @FeatureFlag
  @Option(
      names = {"--compressionCodecName"},
      description =
          "Compression codec for parquet. Valid values: ${COMPLETION-CANDIDATES}. Default SNAPPY")
  public CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;

  @FeatureFlag
  @Option(
      names = {"--sideOutputDebugLogging"},
      negatable = true,
      description = "Whether to write side output debug logging.  Default=false")
  public boolean sideOutputDebugLogging = false;

  @FeatureFlag
  @Option(
      names = {"--disableS3Sink"},
      description =
          "Flag to disable specific S3 sinks.  This is a flag in case writing causes performance issues.  Defaults to empty.")
  public Set<String> disableS3Sink = new HashSet<>();
  // The S3 sink does not support getTransformation, so we need to track the uids separately.
  @VisibleForTesting public ArrayList<String> sinkTransformationUids = new ArrayList<>();

  public S3FileOutput(BaseFlinkJob job, S3Segment s3) {
    this.job = job;
    this.s3 = s3;
  }

  private static <T> FileSink<T> forBulkFormat(
      S3Path path, BulkWriter.Factory<T> factory, SerializableToLongFunction<T> getTimestamp) {
    return FileSink.forBulkFormat(new Path(path.toString()), factory)
        .withBucketAssigner(new DateHourBucketAssigner<T>(getTimestamp))
        .withRollingPolicy(OnCheckpointRollingPolicy.build())
        .build();
  }

  private static <T> BulkWriter.Factory<T> getGenericRecordParquetWriterFactory(Schema schema) {
    ParquetBuilder<T> builder =
        (out) ->
            AvroParquetWriter.<T>builder(out)
                .withSchema(schema)
                .withDataModel(GenericData.get())
                .build();
    return new ParquetWriterFactory<T>(builder);
  }

  // TODO - how to prevent it being set badly?

  // Currently has issues with serialization due to the underlying proto objects not having mappings
  // in avro.
  private static <T extends GeneratedMessageV3> BulkWriter.Factory<T> getAvroParquetWriterFactory(
      Class<T> clazz) {
    ParquetBuilder<T> builder =
        (out) ->
            AvroParquetWriter.<T>builder(out)
                .withSchema(FixedProtobufData.get().getSchema(clazz))
                .withDataModel(FixedProtobufData.get())
                .build();
    return new ParquetWriterFactory<T>(builder);
  }

  private static <T extends GeneratedMessageV3> BulkWriter.Factory<T> getAvroWriterFactory(
      Class<T> protoClass) {
    return new AvroWriterFactory<T>(
        (AvroBuilder<T>)
            out -> {
              // Replace recursive schema classes (like Struct) with non-recursive alternatives.
              // Required for Athena use, which does not support recursive schemas.
              Schema schema = FixedProtobufData.get().getSchema(protoClass);
              FixedProtobufDatumWriter<T> pbWriter = new FixedProtobufDatumWriter<>(schema);
              DataFileWriter<T> dataFileWriter = new DataFileWriter<>(pbWriter);
              dataFileWriter.setCodec(CodecFactory.snappyCodec());
              dataFileWriter.create(schema, out);
              return dataFileWriter;
            });
  }

  // Produces valid parquet, but not always readable by older and avro based readers.
  private static <T extends GeneratedMessageV3> BulkWriter.Factory<T> getParquetWriterFactory(
      Class<T> clazz, CompressionCodecName compressionCodecName) {
    ParquetBuilder<T> builder =
        (out) -> {
          Configuration conf = new Configuration();
          conf.setBoolean("parquet.proto.writeSpecsCompliant", true);
          conf.setInt("parquet.proto.maxRecursion", 5);
          return new MyParquetWriterBuilder<>(out, clazz, compressionCodecName)
              .withConf(conf)
              .build();
        };
    return new ParquetWriterFactory<T>(builder);
  }

  @Override
  public void validateArgs() {
    if (!(logAvro || logParquet) && job.getJobLabel().isEmpty()) {
      throw new IllegalArgumentException(
          "At least one output is needed: hint --logAvro OR --logParquet");
    }
    LOGGER.info("outputS3Directory={}", s3.getDir().build().toString());
  }

  @Override
  public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableList.of();
  }

  // This function is unit tested.
  public <T extends GeneratedMessageV3> ImmutableList<Transformation> sink(
      DataStream<T> input, SerializableFunction<T, Timing> getTiming, S3Path.Builder pathBuilder) {
    // TODO - should this be the kafka timestamp?
    SerializableToLongFunction<T> getTimestamp =
        (row) -> getTiming.apply(row).getEventApiTimestamp();
    return sink(getTimestamp, input, pathBuilder);
  }

  // This function is unit tested.
  // Reordered args due to type erasure.
  public <T extends GeneratedMessageV3> ImmutableList<Transformation> sink(
      SerializableToLongFunction<T> getTimestamp, DataStream<T> input, S3Path.Builder pathBuilder) {
    ImmutableList.Builder<Transformation> xforms = ImmutableList.builder();
    Class<T> clazz = input.getType().getTypeClass();
    if (logAvro) {
      // avro
      sink(getAvroWriterFactory(clazz), getTimestamp, input, pathBuilder).ifPresent(xforms::add);
    }
    if (logParquet) {
      pathBuilder = pathBuilder.prependSubdirs("parquet");
      sink(getParquetWriterFactory(clazz, compressionCodecName), getTimestamp, input, pathBuilder)
          .ifPresent(xforms::add);
    }
    return xforms.build();
  }

  private <T extends GeneratedMessageV3> Optional<Transformation> sink(
      BulkWriter.Factory<T> writerFactory,
      SerializableToLongFunction<T> getTimestamp,
      DataStream<T> input,
      S3Path.Builder pathBuilder) {
    S3Path path = pathBuilder.build();
    String uid = String.format("sink-s3-%s", String.join("-", path.subDirs()));
    if (disableS3Sink.contains(uid)) {
      LOGGER.info("Disabling output {}", uid);
      return Optional.empty();
    }
    job.add(input.sinkTo(forBulkFormat(path, writerFactory, getTimestamp)), uid);
    sinkTransformationUids.add(uid);
    return Optional.of(input.getTransformation());
  }

  /** Output debug logging using parquet format for querying. */
  public Optional<Transformation> outputDebugLogging(
      String name, DataStream<GenericRecord> logStream, Schema logSchema) {
    if (!sideOutputDebugLogging) return Optional.empty();

    S3Path outputDirectory = s3.getDir("etl-side", "debug", name).build();
    String uid = String.format("sink-s3-%s-debug-logging", name);
    if (disableS3Sink.contains(uid)) {
      LOGGER.info("Disabling output {}", uid);
      return Optional.empty();
    }
    LOGGER.info("outputDebugLogging {} name={} outputDirectory={}", uid, name, outputDirectory);
    BulkWriter.Factory<GenericRecord> factory =
        S3FileOutput.getGenericRecordParquetWriterFactory(logSchema);
    job.add(
        logStream.sinkTo(forBulkFormat(outputDirectory, factory, r -> (long) r.get("timestamp"))),
        uid);

    sinkTransformationUids.add(uid);
    return Optional.of(logStream.getTransformation());
  }

  public <T extends org.apache.avro.specific.SpecificRecordBase>
      Optional<Transformation> outputSpecificAvroRecordParquet(
          DataStream<T> logStream,
          Class<T> specificRecordClass,
          SerializableToLongFunction<T> getTimestamp,
          S3Path path) {
    String uid = String.format("sink-s3-%s", String.join("-", path.subDirs()));
    if (disableS3Sink.contains(uid)) {
      LOGGER.info("Disabling output {}", uid);
      return Optional.empty();
    }
    LOGGER.info("outputAvroSpecificRecordAsParquet {} outputDirectory={}", uid, path);
    BulkWriter.Factory<T> factory = ParquetAvroWriters.forSpecificRecord(specificRecordClass);
    job.add(logStream.sinkTo(forBulkFormat(path, factory, getTimestamp)), uid);

    sinkTransformationUids.add(uid);
    return Optional.of(logStream.getTransformation());
  }

  private static class MyParquetWriterBuilder<T extends Message>
      extends ParquetWriter.Builder<T, MyParquetWriterBuilder<T>> {

    private final Class<T> clazz;

    public MyParquetWriterBuilder(
        OutputFile outputFile, Class<T> clazz, CompressionCodecName compressionCodecName) {
      super(outputFile);
      this.withCompressionCodec(compressionCodecName);
      this.clazz = clazz;
    }

    @Override
    protected MyParquetWriterBuilder<T> self() {
      return this;
    }

    @Override
    protected WriteSupport<T> getWriteSupport(Configuration conf) {
      return new ProtoWriteSupport<>(clazz) {
        @Override
        public void write(T record) {
          // THIS IS IN EXECUTION
          try {
            super.write(record);
          } catch (ClassCastException e) {
            throw new RuntimeException(
                "class is " + clazz + ", my record: " + record.toString(), e);
          }
        }
      };
    }
  }
}
