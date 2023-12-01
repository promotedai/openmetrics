workspace(name = "event")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_python",
    sha256 = "cd6730ed53a002c56ce4e2f396ba3b3be262fd7cb68339f0377a45e8227fe332",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.5.0/rules_python-0.5.0.tar.gz",
)

http_archive(
    name = "com_google_protobuf",
    sha256 = "b10bf4e2d1a7586f54e64a5d9e7837e5188fc75ae69e36f215eb01def4f9721b",
    strip_prefix = "protobuf-3.15.3",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.15.3.tar.gz"],
)

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# io_bazel_rules_docker needs Go.  An easy way to install Go is to install Gazelle.
# Start Go section.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "56d8c5a5c91e1af73eca71a6fab2ced959b67c86d12ba37feedb0a2dfea441a6",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.37.0/rules_go-v0.37.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.37.0/rules_go-v0.37.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "5982e5463f171da99e3bdaeff8c0f48283a7a5f396ec5282910b9e8a49c0dd7e",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.25.0/bazel-gazelle-v0.25.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.25.0/bazel-gazelle-v0.25.0.tar.gz",
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains(version = "1.19.3")

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

go_repository(
    name = "com_github_google_go_containerregistry",
    importpath = "github.com/google/go-containerregistry",
    sha256 = "cadb09cb5bcbe00688c73d716d1c9e774d6e4959abec4c425a1b995faf33e964",
    strip_prefix = "google-go-containerregistry-8a28419",
    type = "tar.gz",
    urls = ["https://api.github.com/repos/google/go-containerregistry/tarball/8a2841911ffee4f6892ca0083e89752fb46c48dd"],  # v0.1.4
)
# End Go section.

http_archive(
    name = "rules_java",
    sha256 = "9b87757af5c77e9db5f7c000579309afae75cf6517da745de01ba0c6e4870951",
    url = "https://github.com/bazelbuild/rules_java/releases/download/5.4.0/rules_java-5.4.0.tar.gz",
)

load("@rules_java//java:repositories.bzl", "rules_java_dependencies", "rules_java_toolchains")

rules_java_dependencies()

rules_java_toolchains()

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "rules_proto_grpc",
    sha256 = "507e38c8d95c7efa4f3b1c0595a8e8f139c885cb41a76cab7e20e4e67ae87731",
    strip_prefix = "rules_proto_grpc-4.1.1",
    urls = ["https://github.com/rules-proto-grpc/rules_proto_grpc/archive/4.1.1.tar.gz"],
)

load("@rules_proto_grpc//:repositories.bzl", "rules_proto_grpc_repos", "rules_proto_grpc_toolchains")

rules_proto_grpc_toolchains()

rules_proto_grpc_repos()

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

load("@rules_proto_grpc//python:repositories.bzl", rules_proto_grpc_python_repos = "python_repos")

rules_proto_grpc_python_repos()

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

SCHEMA_COMMIT = "7c57cd7d9baab381fbb2d38fba5aea95523f5183"

# Java.

git_repository(
    name = "com_github_promotedai_schema_internal_git",
    commit = SCHEMA_COMMIT,
    remote = "ssh://git@github.com/promotedai/schema-internal",
)

git_repository(
    name = "com_github_johnynek_bazel_jar_jar",
    commit = "55e3d3bf454641c496e592d77537cf8e65b241c0",
    remote = "https://github.com/johnynek/bazel_jar_jar.git",
)

load("@com_github_johnynek_bazel_jar_jar//:jar_jar.bzl", "jar_jar_repositories")

jar_jar_repositories()

RULES_JVM_EXTERNAL_TAG = "4.1"

RULES_JVM_EXTERNAL_SHA = "f36441aa876c4f6427bfb2d1f2d723b48e9d930b62662bf723ddfb8fc80f0140"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

load("@rules_jvm_external//:defs.bzl", "artifact", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

AWS_JAVA_SDK_1_VERSION = "1.12.413"

AWS_JAVA_SDK_2_VERSION = "2.17.186"

# When version bumping, please update the "flink" container_pull definition near the bottom of this file.
# Use the correct sha256 for the appropriate scala and flink version defined here.
FLINK_VERSION = "1.17.1"

FLINK_MAJOR_VERSION = "1.17"

FLINK_KINESIS_VERSION = "4.1.0-%s" % FLINK_MAJOR_VERSION

PAIMON_VERSION = "0.5.0-incubating"
#PAIMON_VERSION = "0.4.0-incubating"

HIVE_VERSION = "3.1.3"

SCALA_VERSION = "2.12"

AWS_KINESIS_DATA_ANALYTICS_VERSION = "1.1.0"

JACKSON_VERSION = "2.11.2"

HADOOP_VERSION = "3.3.4"

AVRO_VERSION = "1.10.1"

PARQUET_VERSION = "1.12.3"

TEST_CONTAINER_VERSION = "1.17.6"

LOG4J_VERSION = "2.20.0"

GRPC_VERSION = "1.57.2"

# TODO - Move Flink deps from maven to maven_test and maven_neverlink
# https://github.com/bazelbuild/rules_jvm_external

# gRPC
maven_install(
    name = "maven_neverlink",
    artifacts = [
        maven.artifact(
            artifact = "flink-streaming-java",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-core",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-file-sink-common",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-java",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-runtime",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-clients",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-table-api-java-uber",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-table-runtime",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-file-sink-common",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-connector-files",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "jsr305",
            group = "com.google.code.findbugs",
            neverlink = True,
            version = "3.0.2",
        ),
        maven.artifact(
            # picked from https://github.com/apache/flink/pull/21747 to support Mac M1 chip
            artifact = "frocksdbjni",
            group = "com.ververica",
            neverlink = True,
            version = "6.20.3-ververica-2.0",
        ),
        maven.artifact(
            artifact = "flink-statebackend-rocksdb",
            exclusions = [
                "com.ververica:frocksdbjni",
            ],
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-csv",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-metrics-prometheus",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "parquet-common",
            group = "org.apache.parquet",
            neverlink = True,
            version = PARQUET_VERSION,
        ),
        maven.artifact(
            artifact = "parquet-hadoop",
            group = "org.apache.parquet",
            neverlink = True,
            version = PARQUET_VERSION,
        ),
        maven.artifact(
            artifact = "flink-shaded-jackson",
            group = "org.apache.flink",
            neverlink = True,
            version = "2.13.4-16.1",
        ),
        maven.artifact(
            artifact = "flink-protobuf",
            group = "org.apache.flink",
            neverlink = True,
            version = "1.17.0",
        ),
        maven.artifact(
            artifact = "flink-annotations",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-metrics-core",
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "log4j-api",
            group = "org.apache.logging.log4j",
            neverlink = True,
            version = LOG4J_VERSION,
        ),
    ],
    #    fetch_sources = True,
    repositories = [
        "https://maven.google.com",
        "https://repo1.maven.org/maven2",
    ],
    # Double the timeout to see if the 504si, that GitHub Actions is seeing, go away.
    resolve_timeout = 1200,
    strict_visibility = True,
)

maven_install(
    name = "maven_test",
    artifacts = [
        maven.artifact(
            testonly = True,
            artifact = "junit-jupiter",
            group = "org.testcontainers",
            version = TEST_CONTAINER_VERSION,
        ),
        maven.artifact(
            testonly = True,
            artifact = "testcontainers",
            group = "org.testcontainers",
            version = TEST_CONTAINER_VERSION,
        ),
        maven.artifact(
            testonly = True,
            artifact = "kafka",
            group = "org.testcontainers",
            version = TEST_CONTAINER_VERSION,
        ),
        maven.artifact(
            # picked from https://github.com/apache/flink/pull/21747 to support Mac M1 chip
            testonly = True,
            artifact = "frocksdbjni",
            group = "com.ververica",
            version = "6.20.3-ververica-2.0",
        ),
        maven.artifact(
            testonly = True,
            artifact = "flink-csv",
            group = "org.apache.flink",
            version = FLINK_VERSION,
        ),
        maven.artifact(
            testonly = True,
            artifact = "flink-statebackend-rocksdb",
            exclusions = [
                "com.ververica:frocksdbjni",
            ],
            group = "org.apache.flink",
            version = FLINK_VERSION,
        ),
        maven.artifact(
            testonly = True,
            artifact = "flink-table-runtime",
            group = "org.apache.flink",
            version = FLINK_VERSION,
        ),
        maven.artifact(
            testonly = True,
            artifact = "flink-table-planner_%s" % SCALA_VERSION,
            group = "org.apache.flink",
            version = FLINK_VERSION,
        ),
        maven.artifact(
            testonly = True,
            artifact = "flink-test-utils",
            exclusions = [
                "org.junit.jupiter:junit-jupiter",
                "org.apache.logging.log4j:log4j-api",
                "org.apache.logging.log4j:log4j-core",
                "org.apache.logging.log4j:log4j-slf4j-impl",
                "org.testcontainers:testcontainers",
            ],
            group = "org.apache.flink",
            version = FLINK_VERSION,
        ),
        maven.artifact(
            testonly = True,
            artifact = "flink-metrics-prometheus",
            group = "org.apache.flink",
            version = FLINK_VERSION,
        ),
        # change this to log4j-slf4j2-impl for slf4j v2+
        maven.artifact(
            testonly = True,
            artifact = "log4j-slf4j-impl",
            group = "org.apache.logging.log4j",
            version = LOG4J_VERSION,
        ),
        maven.artifact(
            testonly = True,
            artifact = "log4j-1.2-api",
            group = "org.apache.logging.log4j",
            version = LOG4J_VERSION,
        ),
        maven.artifact(
            testonly = True,
            artifact = "log4j-core",
            group = "org.apache.logging.log4j",
            version = LOG4J_VERSION,
        ),
        maven.artifact(
            testonly = True,
            artifact = "log4j-api",
            group = "org.apache.logging.log4j",
            version = LOG4J_VERSION,
        ),
        maven.artifact(
            testonly = True,
            artifact = "log4j-layout-template-json",
            group = "org.apache.logging.log4j",
            version = LOG4J_VERSION,
        ),
    ],
    #    fetch_sources = True,
    repositories = [
        "https://maven.google.com",
        "https://repo1.maven.org/maven2",
    ],
    # Double the timeout to see if the 504si, that GitHub Actions is seeing, go away.
    resolve_timeout = 1200,
)

maven_install(
    artifacts = [
        "org.apache.flink:flink-java:%s" % FLINK_VERSION,
        "org.apache.flink:flink-streaming-java:%s" % FLINK_VERSION,
        maven.artifact(
            artifact = "flink-streaming-java",
            classifier = "tests",
            group = "org.apache.flink",
            version = FLINK_VERSION,
        ),
        #        "org.apache.flink:flink-streaming-scala_%s:%s" % (SCALA_VERSION, FLINK_VERSION),
        "org.apache.flink:flink-file-sink-common:%s" % FLINK_VERSION,
        "org.apache.flink:flink-connector-files:%s" % FLINK_VERSION,
        "org.apache.flink:flink-connector-kafka:%s" % FLINK_VERSION,
        maven.artifact(
            artifact = "flink-connector-kinesis",
            exclusions = [
                "*:*",
            ],
            group = "org.apache.flink",
            version = FLINK_KINESIS_VERSION,
        ),
        "org.apache.flink:flink-clients:%s" % FLINK_VERSION,
        "org.apache.flink:flink-avro:%s" % FLINK_VERSION,
        # We need the latest flink-protobuf version for some bug fix
        "org.apache.flink:flink-protobuf:%s" % FLINK_VERSION,
        "org.apache.avro:avro-protobuf:%s" % AVRO_VERSION,
        "org.apache.flink:flink-json:%s" % FLINK_VERSION,
        maven.artifact(
            artifact = "hive-exec",
            exclusions = [
                "org.apache.curator:apache-curator",
                "commons-lang:commons-lang",
                "org.apache.commons:commons-lang3",
                "com.google.guava:guava",
                "org.eclipse.jetty.aggregate:*",
                "javax.mail:mail",
                "org.apache.zookeeper:zookeeper",
                "org.pentaho:*",
                "com.esotericsoftware:kryo-shaded",
                "org.apache.hbase:*",
                "org.apache.calcite:*",
                "org.apache.calcite.avatica:*",
            ],
            group = "org.apache.hive",
            version = HIVE_VERSION,
        ),
        maven.artifact(
            artifact = "hive-common",
            exclusions = [
                "org.apache.logging.log4j:log4j-slf4j-impl",
            ],
            group = "org.apache.hive",
            version = HIVE_VERSION,
        ),
        "org.apache.flink:flink-runtime-web:%s" % FLINK_VERSION,
        "org.apache.flink:flink-parquet:%s" % FLINK_VERSION,
        maven.artifact(
            artifact = "flink-table-api-java-uber",
            group = "org.apache.flink",
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-test-utils",
            exclusions = [
                "org.junit.jupiter:junit-jupiter",
            ],
            group = "org.apache.flink",
            version = FLINK_VERSION,
        ),
        "com.google.protobuf:protobuf-java-util:3.15.3",
        maven.artifact(
            artifact = "flink-runtime",
            classifier = "tests",
            group = "org.apache.flink",
            version = FLINK_VERSION,
        ),
        "javax.annotation:javax.annotation-api:1.3.2",
        "io.lettuce:lettuce-core:6.2.3.RELEASE",
        "software.amazon.awssdk:bom:%s" % AWS_JAVA_SDK_2_VERSION,
        "software.amazon.awssdk:kms:%s" % AWS_JAVA_SDK_2_VERSION,
        "software.amazon.awssdk:regions:%s" % AWS_JAVA_SDK_2_VERSION,
        "software.amazon.awssdk:sdk-core:%s" % AWS_JAVA_SDK_2_VERSION,
        "software.amazon.awssdk:secretsmanager:%s" % AWS_JAVA_SDK_2_VERSION,
        "com.amazonaws:aws-java-sdk-glue:%s" % AWS_JAVA_SDK_1_VERSION,
        "com.amazonaws:aws-java-sdk-core:%s" % AWS_JAVA_SDK_1_VERSION,
        "com.amazonaws:aws-java-sdk-s3:%s" % AWS_JAVA_SDK_1_VERSION,
        "org.lz4:lz4-java:1.8.0",
        # A bunch needed to resolve Hadoop dependencies.
        "org.apache.hadoop:hadoop-aws:%s" % HADOOP_VERSION,
        "org.apache.commons:commons-lang3:3.11",
        "com.github.ben-manes.caffeine:caffeine:2.9.3",
        "com.google.auto.value:auto-value:1.8.2",
        "com.google.auto.value:auto-value-annotations:1.8.2",
        "com.google.guava:guava:31.0.1-jre",
        "com.google.truth:truth:1.1.3",
        "com.google.truth.extensions:truth-java8-extension:1.1.3",
        "com.google.truth.extensions:truth-proto-extension:1.1.3",
        "org.apache.flink:flink-hadoop-compatibility_%s:%s" % (SCALA_VERSION, FLINK_VERSION),
        "org.apache.paimon:paimon-flink-%s:%s" % (FLINK_MAJOR_VERSION, PAIMON_VERSION),
        maven.artifact(
            artifact = "hadoop-client",
            group = "org.apache.hadoop",
            version = HADOOP_VERSION,
        ),
        maven.artifact(
            artifact = "hadoop-common",
            group = "org.apache.hadoop",
            version = HADOOP_VERSION,
        ),
        "com.twitter:chill-protobuf:0.10.0",
        "com.google.protobuf:protobuf-java:3.15.3",
        "com.twitter:chill_%s:0.10.0" % SCALA_VERSION,
        "com.twitter:chill-java:0.10.0",
        "org.apache.parquet:parquet-avro:%s" % PARQUET_VERSION,
        "ai.promoted:parquet-protobuf:%s" % PARQUET_VERSION,
        "com.github.ua-parser:uap-java:1.5.2",
        "com.ibm.icu:icu4j:4.6.1",
        "org.junit.jupiter:junit-jupiter-api:5.7.0",
        "org.apache.commons:commons-lang3:3.11",

        # TODO - not sure if these are used.
        "org.junit.platform:junit-platform-commons:1.7.0",
        "org.junit.platform:junit-platform-console:1.7.0",
        "com.fasterxml.jackson.core:jackson-core:%s" % JACKSON_VERSION,
        "com.fasterxml.jackson.core:jackson-databind:%s" % JACKSON_VERSION,
        "com.fasterxml.jackson.dataformat:jackson-dataformat-csv:%s" % JACKSON_VERSION,
        "info.picocli:picocli:4.5.1",
        "org.mockito:mockito-core:3.10.0",
        "org.json:json:20200518",
        maven.artifact(
            artifact = "grpc-netty-shaded",
            exclusions = [
                "com.google.guava:guava",
            ],
            group = "io.grpc",
            version = GRPC_VERSION,
        ),
        maven.artifact(
            artifact = "grpc-stub",
            exclusions = [
                "com.google.guava:guava",
            ],
            group = "io.grpc",
            version = GRPC_VERSION,
        ),
        maven.artifact(
            artifact = "grpc-api",
            exclusions = [
                "com.google.guava:guava",
            ],
            group = "io.grpc",
            version = GRPC_VERSION,
        ),
    ],
    excluded_artifacts = [
        # globally exclude logging frameworks
        "org.slf4j:slf4j-reload4j",
        "log4j:log4j",
        "org.apache.logging.log4j:*",
    ],
    #    fetch_sources = True,
    repositories = [
        "https://maven.google.com",
        "https://repo1.maven.org/maven2",
        "https://raw.githubusercontent.com/promotedai/maven-repo/main",
        "https://repository.apache.org/content/repositories/snapshots/",
    ],
    # Double the timeout to see if the 504si, that GitHub Actions is seeing, go away.
    resolve_timeout = 1200,
)

# Maven repos that are not for Flink jobs.  These can include more dependencies.
maven_install(
    name = "maven_nonflink",
    artifacts = [
        "org.apache.logging.log4j:log4j-api:%s" % LOG4J_VERSION,
        "org.apache.logging.log4j:log4j-core:%s" % LOG4J_VERSION,
        "org.apache.logging.log4j:log4j-layout-template-json:%s" % LOG4J_VERSION,
        # change this to log4j-slf4j2-impl for slf4j v2+
        "org.apache.logging.log4j:log4j-slf4j-impl:%s" % LOG4J_VERSION,
    ],
    repositories = [
        "https://maven.google.com",
        "https://repo1.maven.org/maven2",
        "https://raw.githubusercontent.com/promotedai/maven-repo/main",
    ],
    # Double the timeout to see if the 504si, that GitHub Actions is seeing, go away.
    resolve_timeout = 1200,
)

load(":junit5.bzl", "junit_jupiter_java_repositories", "junit_platform_java_repositories")

JUNIT_JUPITER_VERSION = "5.6.0"

JUNIT_PLATFORM_VERSION = "1.6.0"

junit_jupiter_java_repositories(
    version = JUNIT_JUPITER_VERSION,
)

junit_platform_java_repositories(
    version = JUNIT_PLATFORM_VERSION,
)

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "95d39fd84ff4474babaf190450ee034d958202043e366b9fc38f438c9e6c3334",
    strip_prefix = "rules_docker-0.16.0",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.16.0/rules_docker-v0.16.0.tar.gz"],
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

load("@io_bazel_rules_docker//repositories:py_repositories.bzl", "py_deps")

py_deps()

load(
    "@io_bazel_rules_docker//container:container.bzl",
    "container_pull",
)
load(
    "@io_bazel_rules_docker//java:image.bzl",
    _java_image_repos = "repositories",
)

_java_image_repos()

load(
    "@io_bazel_rules_docker//container:container.bzl",
    "container_pull",
)

container_pull(
    name = "java_base",
    # Java11
    digest = "sha256:629d4fdc17eec821242d45497abcb88cc0442c47fd5748baa79d88dde7da3e2d",
    registry = "gcr.io",
    repository = "distroless/java",
)

container_pull(
    name = "java_debug",
    # Java11 debug
    digest = "sha256:80f87dcce03ba2591d777471818ab77f6fb580faa86628d2f885f7700af7941b",
    registry = "gcr.io",
    repository = "distroless/java",
)

# FLINK_VERSION = this needs updating when we increment the Flink version.
container_pull(
    name = "flink",
    # grab the digest by running `docker manifest inspect flink:[tag]`
    digest = "sha256:ac1256909e06a5bf8d91c92037bc322580e0c11995d0844f6876d05090c737c1",
    registry = "index.docker.io",
    repository = "library/flink",
    # tag = "1.17.1-java11",
)

container_pull(
    name = "flink-arm",
    # grab the digest by running `docker manifest inspect flink:[tag]`
    digest = "sha256:bf975abc2b9307cbb2080193f666a8c8635cb8372aeecbb64a295107db441b5f",
    registry = "index.docker.io",
    repository = "library/flink",
    # tag = "1.17.1-java11",
)

RULES_AVRO_COMMIT = "03a3148d0af92a430bfa74fed1c8e6abb0685c8c"

RULES_AVRO_SHA256 = "df0be97b1be6332c5843e3062f8b232351e5b0537946c90e308c194a4f524c87"

# Dan reviewed this Tar.  It's small.  Looks safe.
http_archive(
    name = "io_bazel_rules_avro",
    sha256 = RULES_AVRO_SHA256,
    strip_prefix = "rules_avro-%s" % RULES_AVRO_COMMIT,
    url = "https://github.com/chenrui333/rules_avro/archive/%s.tar.gz" % RULES_AVRO_COMMIT,
)

load("@io_bazel_rules_avro//avro:avro.bzl", "avro_repositories")

avro_repositories(version = AVRO_VERSION)

load("@rules_proto_grpc//java:repositories.bzl", rules_proto_grpc_java_repos = "java_repos")

rules_proto_grpc_java_repos()

load("@io_grpc_grpc_java//:repositories.bzl", "IO_GRPC_GRPC_JAVA_ARTIFACTS", "IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS", "grpc_java_repositories")

grpc_java_repositories()

maven_install(
    name = "maven_grpc",
    artifacts = IO_GRPC_GRPC_JAVA_ARTIFACTS,
    generate_compat_repositories = True,
    override_targets = IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS,
    repositories = [
        "https://repo.maven.apache.org/maven2/",
    ],
)

load("@maven_grpc//:compat.bzl", "compat_repositories")

compat_repositories()

# Csharp needed because schema-internal BUILD files have the reference.
load("@rules_proto_grpc//csharp:repositories.bzl", rules_proto_grpc_csharp_repos = "csharp_repos")

rules_proto_grpc_csharp_repos()

load("@io_bazel_rules_dotnet//dotnet:deps.bzl", "dotnet_repositories")

dotnet_repositories()

load(
    "@io_bazel_rules_dotnet//dotnet:defs.bzl",
    "dotnet_register_toolchains",
    "dotnet_repositories_nugets",
)

dotnet_register_toolchains()

dotnet_repositories_nugets()
