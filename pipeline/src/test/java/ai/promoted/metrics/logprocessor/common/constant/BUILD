package(default_visibility = ["//src/test:__subpackages__"])

load("//:junit5.bzl", "java_junit5_test")

# Use java_test after bazel officially support junit5
java_junit5_test(
    name = "tests",
    timeout = "short",
    srcs = glob(["*.java"]),
    test_package = "ai.promoted.metrics.logprocessor.common.constant",
    deps = [
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/constant",
    ],
)
