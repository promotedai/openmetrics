package(default_visibility = ["//src/test:__subpackages__"])

load("//:junit5.bzl", "java_junit5_test")

# Use java_test after bazel officially support junit5
java_junit5_test(
    name = "inferred_tests",
    timeout = "short",
    use_testrunner = False,
    test_package = "ai.promoted.metrics.logprocessor.common.functions.inferred",
    srcs = glob(["*.java"]),
    main_class = "com.flexport.bazeljunit5.BazelJUnit5ConsoleLauncher",
    deps = [
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/error:avro",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/flink/operator",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/functions",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/functions/inferred",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/testing",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/util",
        "//pipeline/src/main/java/com/flexport/bazeljunit5",
        "@com_github_promotedai_schema_internal_git//proto/common:common_java_proto",
        "@com_github_promotedai_schema_internal_git//proto/event:event_java_proto",
        "@com_google_protobuf//java/core",
        "@maven//:com_esotericsoftware_kryo_kryo",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:com_twitter_chill_protobuf",
        "@maven//:org_apache_flink_flink_core",
        "@maven_neverlink//:org_apache_flink_flink_java",
        "@maven//:org_apache_flink_flink_streaming_java",
        "@maven//:org_apache_flink_flink_streaming_java_tests",
        "@maven//:org_apache_flink_flink_test_utils",
        "@maven//:org_mockito_mockito_core",
    ],
    jvm_flags = [
        "-Dlog4j.configurationFile=resources/log4j2-test.xml",
    ],
    resources = ["//pipeline/src/resources:log4j2-test.xml"],
)
