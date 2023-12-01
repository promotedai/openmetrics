package(default_visibility = ["//src/test:__subpackages__"])

load("//:junit5.bzl", "java_junit5_test")

# Use java_test after bazel officially support junit5
java_junit5_test(
    name = "tests",
    timeout = "short",
    srcs = glob(["*.java"]),
    test_package = "ai.promoted.metrics.logprocessor.common.records",
    deps = [
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/records",
        "@com_github_promotedai_schema_internal_git//proto/event:event_java_proto",
        "@maven//:org_apache_flink_flink_core",
        "@maven//:org_apache_flink_flink_streaming_java",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_mockito_mockito_core",
    ],
)
