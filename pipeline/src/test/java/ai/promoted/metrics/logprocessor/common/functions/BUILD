package(default_visibility = ["//src/test:__subpackages__"])

load("//:junit5.bzl", "java_junit5_test")

# Use java_test after bazel officially support junit5
java_junit5_test(
    name = "tests",
    srcs = glob(["*.java"]),
    test_package = "ai.promoted.metrics.logprocessor.common.functions",
    deps = [
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/counter",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/constant",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/error:avro",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/functions",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/testing",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/util",
        "//pipeline/src/main/java/com/flexport/bazeljunit5",
        "@com_github_promotedai_schema_internal_git//proto/common:common_java_proto",
        "@com_github_promotedai_schema_internal_git//proto/delivery/private/features:features_java_proto",
        "@com_github_promotedai_schema_internal_git//proto/event:event_java_proto",
        "@com_github_promotedai_schema_internal_git//proto/promotion:promotion_java_proto",
        "@com_google_protobuf//java/core",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:com_twitter_chill_protobuf",
        "@maven//:org_apache_flink_flink_core",
        "@maven_neverlink//:org_apache_flink_flink_java",
        "@maven//:org_apache_flink_flink_streaming_java",
        "@maven//:org_apache_flink_flink_streaming_java_tests",
        "@maven//:org_apache_flink_flink_test_utils",
        "@maven//:org_mockito_mockito_core",
        "@maven_test//:org_apache_flink_flink_statebackend_rocksdb",
        # Manually replace the frocksdbjni version to support mac m1 chip https://github.com/apache/flink/pull/21747
        "@maven_test//:com_ververica_frocksdbjni_6_20_3_ververica_2_0",
    ],
)
