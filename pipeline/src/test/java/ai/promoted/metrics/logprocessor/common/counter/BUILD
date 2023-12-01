package(default_visibility = ["//src/test:__subpackages__"])

load("//:junit5.bzl", "java_junit5_test")

# Use java_test after bazel officially support junit5
java_junit5_test(
    name = "tests",
    timeout = "short",
    srcs = glob(["*.java"]),
    test_package = "ai.promoted.metrics.logprocessor.common.counter",
    jvm_flags = [
        "-Dlog4j.configurationFile=resources/log4j2-test.xml",
    ],
    resources = ["//pipeline/src/resources:log4j2-test.xml"],
    deps = [
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/counter",
        "@maven//:org_apache_flink_flink_core",
        "@maven//:org_apache_flink_flink_streaming_java",
        "@maven//:org_apache_flink_flink_streaming_java_tests",
        "@maven//:org_apache_flink_flink_test_utils",
        "@maven_test//:org_apache_logging_log4j_log4j_api",
        "@maven_test//:org_apache_logging_log4j_log4j_slf4j_impl",
    ],
)
