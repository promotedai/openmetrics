package(default_visibility = ["//src/test:__subpackages__"])

load("//:junit5.bzl", "java_junit5_test")

# Use java_test after bazel officially support junit5
java_junit5_test(
    name = "tests",
    timeout = "short",
    srcs = glob(["*.java"]),
    test_package = "ai.promoted.metrics.logprocessor.common.testing",
    deps = [
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/fakedatagenerator",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/testing",
        "@com_github_promotedai_schema_internal_git//proto/delivery:delivery_java_proto",
        "@com_github_promotedai_schema_internal_git//proto/event:event_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:info_picocli_picocli",
        "@maven//:org_apache_avro_avro",
    ],
)
