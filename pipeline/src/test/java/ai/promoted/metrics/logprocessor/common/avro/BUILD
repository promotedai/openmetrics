package(default_visibility = ["//src/test:__subpackages__"])

load("//:junit5.bzl", "java_junit5_test")

# Use java_test after bazel officially support junit5
java_junit5_test(
    name = "tests",
    timeout = "short",
    srcs = glob(["*.java"]),
    test_package = "ai.promoted.metrics.logprocessor.common.avro",
    deps = [
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/avro",
        "@com_github_promotedai_schema_internal_git//proto/common:common_java_proto",
        "@com_github_promotedai_schema_internal_git//proto/event:event_java_proto",
        "@com_google_protobuf//java/core",
        "@maven//:org_apache_avro_avro",
        "@maven//:org_apache_avro_avro_protobuf",
        "@maven_test//:org_apache_logging_log4j_log4j_api",
    ],
)
