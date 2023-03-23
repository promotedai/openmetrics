load("@rules_java//java:defs.bzl", "java_library")

filegroup(
    name = "java_files",
    srcs = glob(["**/*.java"]),
)

java_library(
    name = "bazeljunit5",
    srcs = [
        ":java_files",
    ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@maven//:org_junit_platform_junit_platform_commons",
        "@maven//:org_junit_platform_junit_platform_console",
        "@maven//:com_twitter_chill_2_12",
    ],
)
