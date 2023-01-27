package ai.promoted.metrics.logprocessor.common.job.testing;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.FeatureFlag;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.testing.ProtoAsserts;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.client.deployment.executors.RemoteExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;

/** ABC for a Flink job unit test. */
public abstract class BaseJobUnitTest<JOB extends BaseFlinkJob> extends BaseJobTest<JOB> {

  @Override
  protected Configuration getClientConfiguration() {
    Configuration result = new Configuration();
    result.set(DeploymentOptions.TARGET, RemoteExecutor.NAME);
    return result;
  }

  /**
   * This attempts to catch recursive (including cyclic) types for Glue.
   * If you need to change this test, please also change {@code FixedProtobufData}.
   */
  @Test
  public void checkForRecursiveProtoTypes() throws Exception {
    JOB job = createJob();
    for (Class<? extends GeneratedMessageV3> clazz : job.getProtoClasses()) {
      Descriptors.Descriptor descriptor = (Descriptors.Descriptor) clazz.getMethod("getDescriptor").invoke(null);
      ProtoAsserts.assertNoUnknownRecursiveTypes(descriptor, ImmutableSet.of(
              "google.protobuf.ListValue",
              "google.protobuf.Struct",
              "google.protobuf.Value",
              "delivery.QualityScoreTerm"
      ));
    }
  }

  /**
   * This test attempts to check the execution plan to see if we have uids set on the nodes.
   * This won't accurately catch issues.  It will miss some cases where we should add uids.  It will also
   * flag in cases where we cannot add uids.  This test is better than nothing.  The goal is to flag major graph
   * changes to the dev and reviewer.
   */
  @Test
  public void checkForUids_featureFlagsDefault() throws Exception {
    JOB job = createJob();
    new UidChecker().check(job);
  }

  @Test
  public void checkForUids_featureFlagsOff() throws Exception {
    JOB job = createJob();
    setFeatureFlags(job, false);
    new UidChecker().check(job);
  }

  @Test
  public void checkForUids_featureFlagsOn() throws Exception {
    JOB job = createJob();
    setFeatureFlags(job, true);
    new UidChecker().check(job);
  }

  private void setFeatureFlags(FlinkSegment segment, boolean enable) throws Exception {
    for (Field field : segment.getClass().getDeclaredFields()) {
      if (field.isAnnotationPresent(CommandLine.Mixin.class) && FlinkSegment.class.isAssignableFrom(field.getType())) {
        setFeatureFlags((FlinkSegment)field.get(segment), enable);
      } else if (field.isAnnotationPresent(FeatureFlag.class)) {
        if (field.getType().equals(Boolean.class) || field.getType().equals(boolean.class)) {
          field.setBoolean(segment, enable);
        } else if (field.getType().equals(String.class)) {
          switch (field.getName()) {
            case "counterEndpoints":
              // Fake string to trigger code paths.
              field.set(segment, enable ? "fakeredispath": "");
              break;
            case "contentApiRootUrl":
              // Fake string to trigger code paths.
              field.set(segment, enable ? "http://localhost:5150/": "");
              break;
            default:
              throw new UnsupportedOperationException(String.format("Unsupported type for %s FeatureFlag %s",
                      field.getType(), field.getName()));
          }
        } else if (field.getType().equals(List.class)) {
          switch (field.getName()) {
            case "nonBuyerUserSparseHashes":
              // Fake list to trigger code paths.
              // User property "is_host" - ccc
              field.set(segment, enable ? ImmutableList.of(6102540093495235004L): ImmutableList.of());
              break;
            default:
              throw new UnsupportedOperationException(String.format("Unsupported type for %s FeatureFlag %s",
                      field.getType(), field.getName()));
          }
        } else if (field.getType().equals(Set.class)) {
          switch (field.getName()) {
            case "disableS3Sink":
              // TODO - pick a UID that actually impacts this.
              field.set(segment, enable ? ImmutableSet.of("sink-s3-fake-sink"): ImmutableSet.of());
              break;
            default:
              throw new UnsupportedOperationException(String.format("Unsupported type for %s FeatureFlag %s",
                      field.getType(), field.getName()));
          }
        } else if (field.getType().equals(CompressionCodecName.class)) {
          switch (field.getName()) {
            case "compressionCodecName":
              field.set(segment, enable ? CompressionCodecName.SNAPPY : CompressionCodecName.UNCOMPRESSED);
              break;
            default:
              throw new UnsupportedOperationException(String.format("Unsupported type for %s FeatureFlag %s",
                      field.getType(), field.getName()));
          }
        } else {
          throw new UnsupportedOperationException(String.format("Unsupported type for %s FeatureFlag %s",
                  field.getType(), field.getName()));
        }
      }
    }
  }
}
