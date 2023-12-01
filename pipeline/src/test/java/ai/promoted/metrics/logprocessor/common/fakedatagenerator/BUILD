package(default_visibility = ["//src/test:__subpackages__"])

load("//:junit5.bzl", "java_junit5_test")

# Use java_test after bazel officially support junit5
java_junit5_test(
    name = "tests",
    timeout = "short",
    srcs = glob(["*.java"]),
    data = [
        "createItemShoppingCartBuilder.txt",
        "createStoreInsertionItemShoppingCartBuilder.txt",
        "createStoreToItemBuilder.txt",
    ],
    test_package = "ai.promoted.metrics.logprocessor.common.fakedatagenerator",
    deps = [
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/fakedatagenerator",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/fakedatagenerator/content",
        "@com_github_promotedai_schema_internal_git//proto/event:event_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_flink_flink_core",
    ],
)
