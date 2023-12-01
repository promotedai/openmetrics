package(default_visibility = ["//src/test:__subpackages__"])

load("//:junit5.bzl", "java_junit5_test")

# Use java_test after bazel officially support junit5
java_junit5_test(
    name = "tests",
    timeout = "short",
    srcs = glob(["*.java"]),
    jvm_flags = [
        "-Dlog4j.configurationFile=resources/log4j2-test.xml",
    ],
    main_class = "com.flexport.bazeljunit5.BazelJUnit5ConsoleLauncher",
    resources = ["//pipeline/src/resources:log4j2-test.xml"],
    test_package = "ai.promoted.metrics.logprocessor.common.util",
    use_testrunner = False,
    deps = [
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/error:avro",
        "//pipeline/src/main/java/ai/promoted/metrics/logprocessor/common/util",
        "//pipeline/src/main/java/com/flexport/bazeljunit5",
        "@com_github_promotedai_schema_internal_git//proto/delivery:delivery_java_proto",
        "@com_github_promotedai_schema_internal_git//proto/delivery/private/features:features_java_proto",
        "@com_github_promotedai_schema_internal_git//proto/event:event_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:org_apache_flink_flink_core",
        "@maven//:org_mockito_mockito_core",
        "@maven_test//:org_apache_logging_log4j_log4j_api",
        "@maven_test//:org_apache_logging_log4j_log4j_slf4j_impl",
    ],
)
