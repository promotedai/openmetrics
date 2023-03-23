package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.metrics.logprocessor.common.s3.S3Path;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import java.util.List;
import picocli.CommandLine.Option;

public class S3Segment implements FlinkSegment {
  private final BaseFlinkJob job;

  @Option(
      names = {"-o", "--s3Directory", "--s3OutputDirectory"},
      defaultValue = "",
      description = "Name of the root s3 path (e.g. 's3a://my-bucket/').")
  public String rootPath = "";

  @Option(
      names = {"-s", "--s3Bucket"},
      defaultValue = "",
      description =
          "Name of the s3 bucket.  --s3Directory is higher priority.  Prefer to use that instead.")
  public String bucket = "";

  public S3Segment(BaseFlinkJob job) {
    this.job = job;
  }

  @Override
  public void validateArgs() {}

  @Override
  public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableList.of();
  }

  // TODO - it might not be intuitive when the job label is prefixed in the s3 path.  Figure out
  // a better way to prevent programming errors.

  /** DO NOT pass in subdirs with path delimiters like '/'. This does it for you. */
  public S3Path.Builder getDir(String... subdirs) {
    return getDir(ImmutableList.copyOf(subdirs));
  }

  /** DO NOT pass in subdirs with path delimiters like '/'. This does it for you. */
  public S3Path.Builder getDir(Iterable<String> subdirs) {
    S3Path.Builder builder = getPathBuilder();
    String jobLabel = job.getJobLabel();
    if (!jobLabel.isEmpty()) {
      builder.setJoinLabel(jobLabel);
    }
    return builder.addSubDirs(subdirs);
  }

  /** Return an S3Path.Builder which follows our flink output path conventions. */
  public S3Path.Builder getPathBuilder() {
    S3Path.Builder builder = S3Path.builder();
    if (!rootPath.isEmpty()) {
      builder = builder.setRoot(rootPath);
    } else if (!bucket.isEmpty()) {
      builder = builder.setRoot(Constants.getS3BucketUri(bucket));
    } else {
      throw new IllegalArgumentException("Either s3Bucket or s3OutputDirectory needs to be set");
    }
    return builder;
  }
}
