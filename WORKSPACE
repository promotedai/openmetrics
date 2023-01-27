workspace(name = "event")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_python",
    sha256 = "cd6730ed53a002c56ce4e2f396ba3b3be262fd7cb68339f0377a45e8227fe332",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.5.0/rules_python-0.5.0.tar.gz",
)

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "8e968b5fcea1d2d64071872b12737bbb5514524ee5f0a4f54f5920266c261acb",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.28.0/rules_go-v0.28.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.28.0/rules_go-v0.28.0.zip",
    ],
)

http_archive(
    name = "com_google_protobuf",
    sha256 = "b10bf4e2d1a7586f54e64a5d9e7837e5188fc75ae69e36f215eb01def4f9721b",
    strip_prefix = "protobuf-3.15.3",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.15.3.tar.gz"],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "5982e5463f171da99e3bdaeff8c0f48283a7a5f396ec5282910b9e8a49c0dd7e",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.25.0/bazel-gazelle-v0.25.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.25.0/bazel-gazelle-v0.25.0.tar.gz",
    ],
)

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains(version = "1.17.3")

# load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")
# rules_proto_dependencies()
# rules_proto_toolchains()

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "rules_proto_grpc",
    sha256 = "fa7a59e0d1527ac69be652407b457ba1cb40700752a3ee6cc2dd25d9cb28bb1a",
    strip_prefix = "rules_proto_grpc-3.1.0",
    urls = ["https://github.com/rules-proto-grpc/rules_proto_grpc/archive/3.1.0.tar.gz"],
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

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

go_repository(
    name = "in_gopkg_yaml_v2",
    importpath = "gopkg.in/yaml.v2",
    sum = "h1:/eiJrUcujPVeJ3xlSWaiNi3uSVmDGBK1pDHUHAnao1I=",
    version = "v2.2.4",
)

go_repository(
    name = "com_github_data_dog_go_sqlmock",
    importpath = "github.com/DATA-DOG/go-sqlmock",
    sum = "h1:ThlnYciV1iM/V0OSF/dtkqWb6xo5qITT1TJBG1MRDJM=",
    version = "v1.4.1",
)

go_repository(
    name = "com_github_aws_aws_lambda_go",
    importpath = "github.com/aws/aws-lambda-go",
    sum = "h1:T+u/g79zPKw1oJM7xYhvpq7i4Sjc0iVsXZUaqRVVSOg=",
    version = "v1.6.0",
)

go_repository(
    name = "com_github_go_redis_redis_v7",
    importpath = "github.com/go-redis/redis/v7",
    sum = "h1:AVkqXtvak6eXAvqIA+0rDlh6St/M7/vaf67NEqPhP2w=",
    version = "v7.0.1",
)

go_repository(
    name = "com_github_go_sql_driver_mysql",
    importpath = "github.com/go-sql-driver/mysql",
    sum = "h1:ozyZYNQW3x3HtqT1jira07DN2PArx2v7/mN66gGcHOs=",
    version = "v1.5.0",
)

go_repository(
    name = "com_github_machinebox_graphql",
    importpath = "github.com/machinebox/graphql",
    sum = "h1:dWKpJligYKhYKO5A2gvNhkJdQMNZeChZYyBbrZkBZfo=",
    version = "v0.2.2",
)

go_repository(
    name = "com_github_pkg_errors",
    importpath = "github.com/pkg/errors",
    sum = "h1:FEBLx1zS214owpjy7qsBeixbURkuhQAwrK5UwLGTwt4=",
    version = "v0.9.1",
)

go_repository(
    name = "com_github_sirupsen_logrus",
    importpath = "github.com/sirupsen/logrus",
    sum = "h1:SPIRibHv4MatM3XXNO2BJeFLZwZ2LvZgfQ5+UNI2im4=",
    version = "v1.4.2",
)

go_repository(
    name = "com_github_stretchr_testify",
    importpath = "github.com/stretchr/testify",
    sum = "h1:hDPOHmpOpP40lSULcqw7IrRb/u7w6RpDC9399XyoNd0=",
    version = "v1.6.1",
)

go_repository(
    name = "com_github_nsf_jsondiff",
    importpath = "github.com/nsf/jsondiff",
    sum = "h1:NHrXEjTNQY7P0Zfx1aMrNhpgxHmow66XQtm0aQLY0AE=",
    version = "v0.0.0-20210926074059-1e845ec5d249",
)

go_repository(
    name = "com_github_hashicorp_go_multierror",
    importpath = "github.com/hashicorp/go-multierror",
    sum = "h1:H5DkEtf6CXdFp0N0Em5UCwQpXMWke8IA0+lD48awMYo=",
    version = "v1.1.1",
)

go_repository(
    name = "com_github_hashicorp_errwrap",
    importpath = "github.com/hashicorp/errwrap",
    sum = "h1:OxrOeh75EUXMY8TBjag2fzXGZ40LB6IKw45YeGUDY2I=",
    version = "v1.1.0",
)

go_repository(
    name = "com_github_pierrec_lz4_v4",
    importpath = "github.com/pierrec/lz4/v4",
    sum = "h1:+fL8AQEZtz/ijeNnpduH0bROTu0O3NZAlPjQxGn8LwE=",
    version = "v4.1.14",
)

gazelle_dependencies()

# Autogenerated rules.
# Run the following command to update the dependencies.
# bazel run //:gazelle -- update

go_repository(
    name = "com_github_davecgh_go_spew",
    importpath = "github.com/davecgh/go-spew",
    sum = "h1:vj9j/u1bqnvCEfJOwUhtlOARqs3+rkHYY13jYWTU97c=",
    version = "v1.1.1",
)

go_repository(
    name = "com_github_fsnotify_fsnotify",
    importpath = "github.com/fsnotify/fsnotify",
    sum = "h1:IXs+QLmnXW2CcXuY+8Mzv/fWEsPGWxqefPtCP5CnV9I=",
    version = "v1.4.7",
)

go_repository(
    name = "com_github_golang_protobuf",
    importpath = "github.com/golang/protobuf",
    sum = "h1:+Z5KGCizgyZCbGh1KZqA0fcLLkwbsjIzS4aV2v7wJX0=",
    version = "v1.4.2",
)

go_repository(
    name = "com_github_hpcloud_tail",
    importpath = "github.com/hpcloud/tail",
    sum = "h1:nfCOvKYfkgYP8hkirhJocXT2+zOD8yUNjXaWfTlyFKI=",
    version = "v1.0.0",
)

go_repository(
    name = "com_github_konsorten_go_windows_terminal_sequences",
    importpath = "github.com/konsorten/go-windows-terminal-sequences",
    sum = "h1:mweAR1A6xJ3oS2pRaGiHgQ4OO8tzTaLawm8vnODuwDk=",
    version = "v1.0.1",
)

go_repository(
    name = "com_github_kr_pretty",
    importpath = "github.com/kr/pretty",
    sum = "h1:Fmg33tUaq4/8ym9TJN1x7sLJnHVwhP33CNkpYV/7rwI=",
    version = "v0.2.1",
)

go_repository(
    name = "com_github_kr_pty",
    importpath = "github.com/kr/pty",
    sum = "h1:VkoXIwSboBpnk99O/KFauAEILuNHv5DVFKZMBN/gUgw=",
    version = "v1.1.1",
)

go_repository(
    name = "com_github_kr_text",
    importpath = "github.com/kr/text",
    sum = "h1:45sCR5RtlFHMR4UwH9sdQ5TC8v0qDQCHnXt+kaKSTVE=",
    version = "v0.1.0",
)

go_repository(
    name = "com_github_onsi_ginkgo",
    importpath = "github.com/onsi/ginkgo",
    sum = "h1:q/mM8GF/n0shIN8SaAZ0V+jnLPzen6WIVZdiwrRlMlo=",
    version = "v1.10.1",
)

go_repository(
    name = "com_github_onsi_gomega",
    importpath = "github.com/onsi/gomega",
    sum = "h1:XPnZz8VVBHjVsy1vzJmRwIcSwiUO+JFfrv/xGiigmME=",
    version = "v1.7.0",
)

go_repository(
    name = "com_github_pmezard_go_difflib",
    importpath = "github.com/pmezard/go-difflib",
    sum = "h1:4DBwDE0NGyQoBHbLQYPwSUPoCMWR5BEzIk/f1lZbAQM=",
    version = "v1.0.0",
)

go_repository(
    name = "com_github_stretchr_objx",
    importpath = "github.com/stretchr/objx",
    sum = "h1:2vfRuCMp5sSVIDSqO8oNnWJq7mPa6KVP3iPIwFBuy8A=",
    version = "v0.1.1",
)

go_repository(
    name = "in_gopkg_check_v1",
    importpath = "gopkg.in/check.v1",
    sum = "h1:YR8cESwS4TdDjEe65xsg0ogRM/Nc3DYOhEAlW+xobZo=",
    version = "v1.0.0-20190902080502-41f04d3bba15",
)

go_repository(
    name = "in_gopkg_fsnotify_v1",
    importpath = "gopkg.in/fsnotify.v1",
    sum = "h1:xOHLXZwVvI9hhs+cLKq5+I5onOuwQLhQwiu63xxlHs4=",
    version = "v1.4.7",
)

go_repository(
    name = "in_gopkg_tomb_v1",
    importpath = "gopkg.in/tomb.v1",
    sum = "h1:uRGJdciOHaEIrze2W8Q3AKkepLTh2hOroT7a+7czfdQ=",
    version = "v1.0.0-20141024135613-dd632973f1e7",
)

go_repository(
    name = "org_golang_x_crypto",
    importpath = "golang.org/x/crypto",
    sum = "h1:rlLehGeYg6jfoyz/eDqDU1iRXLKfR42nnNh57ytKEWo=",
    version = "v0.0.0-20190506204251-e1dfcc566284",
)

go_repository(
    name = "org_golang_x_net",
    importpath = "golang.org/x/net",
    sum = "h1:l5EDrHhldLYb3ZRHDUhXF7Om7MvYXnkV9/iQNo1lX6g=",
    version = "v0.0.0-20190923162816-aa69164e4478",
)

go_repository(
    name = "org_golang_x_sync",
    importpath = "golang.org/x/sync",
    sum = "h1:wMNYb4v58l5UBM7MYRLPG6ZhfOqbKu7X5eyFl8ZhKvA=",
    version = "v0.0.0-20180314180146-1d60e4601c6f",
)

go_repository(
    name = "org_golang_x_sys",
    importpath = "golang.org/x/sys",
    sum = "h1:/XfQ9z7ib8eEJX2hdgFTZJ/ntt0swNk5oYBziWeTCvY=",
    version = "v0.0.0-20191010194322-b09406accb47",
)

go_repository(
    name = "org_golang_x_text",
    importpath = "golang.org/x/text",
    sum = "h1:tW2bmiBqwgJj/UpqtC8EpXEZVYOwU0yG4iWbprSVAcs=",
    version = "v0.3.2",
)

go_repository(
    name = "org_golang_x_tools",
    importpath = "golang.org/x/tools",
    sum = "h1:FDhOuMEY4JVRztM/gsbk+IKUQ8kj74bxZrgw87eMMVc=",
    version = "v0.0.0-20180917221912-90fa682c2a6e",
)

go_repository(
    name = "com_github_aws_aws_sdk_go",
    importpath = "github.com/aws/aws-sdk-go",
    sum = "h1:w3O/LGvLCliVFJ2fGrpaWDGbRHj1f+aipB1MMfInN24=",
    version = "v1.29.4",
)

go_repository(
    name = "com_github_jmespath_go_jmespath",
    importpath = "github.com/jmespath/go-jmespath",
    sum = "h1:pmfjZENx5imkbgOkpRUYLnmbU7UEFbjtDA2hxJ1ichM=",
    version = "v0.0.0-20180206201540-c2b33e8439af",
)

SCHEMA_COMMIT = "954c447a02a01e6cf1a105b7c8aaf7cff8ebe893"

GO_COMMON_COMMIT = "77c41b9550782a527b71383c9e1f1b557d405068"

SCHEMA_aaa_COMMIT = "aa2a4f1209b9effb0e0040b477a38c4662616705"

go_repository(
    name = "com_github_promotedai_schema_internal",
    commit = SCHEMA_COMMIT,
    importpath = "github.com/promotedai/schema-internal",
    remote = "ssh://git@github.com/promotedai/schema-internal",
    vcs = "git",
)

go_repository(
    name = "com_github_promotedai_schema_aaa",
    commit = SCHEMA_aaa_COMMIT,
    importpath = "github.com/promotedai/schema-aaa",
    remote = "ssh://git@github.com/promotedai/schema-aaa",
    vcs = "git",
)

go_repository(
    name = "com_github_promotedai_go_common",
    commit = GO_COMMON_COMMIT,
    importpath = "github.com/promotedai/go-common",
    remote = "ssh://git@github.com/promotedai/go-common",
    vcs = "git",
)

go_repository(
    name = "com_github_segmentio_kafka_go",
    importpath = "github.com/segmentio/kafka-go",
    sum = "h1:jIHLImr9J3qycgwHR+cw1x9eLLLYNntpuYPBPjsOc3A=",
    version = "v0.4.30",
)

go_repository(
    name = "com_github_google_subcommands",
    importpath = "github.com/google/subcommands",
    sum = "h1:vWQspBTo2nEqTUFita5/KeEWlUL8kQObDFbub/EN9oE=",
    version = "v1.2.0",
)

go_repository(
    name = "com_github_bradleyjkemp_cupaloy",
    importpath = "github.com/bradleyjkemp/cupaloy",
    sum = "h1:UafIjBvWQmS9i/xRg+CamMrnLTKNzo+bdmT/oH34c2Y=",
    version = "v2.3.0+incompatible",
)

go_repository(
    name = "com_github_linkedin_goavro_v2",
    importpath = "github.com/linkedin/goavro/v2",
    sum = "h1:Vd++Rb/RKcmNJjM0HP/JJFMEWa21eUBVKPYlKehOGrM=",
    version = "v2.9.7",
)

go_repository(
    name = "com_github_golang_snappy",
    importpath = "github.com/golang/snappy",
    sum = "h1:Qgr9rKW7uDUkrbSmQeiDsGa8SjGyCOGtuasMWwvp2P4=",
    version = "v0.0.1",
)

go_repository(
    name = "com_github_google_uuid",
    importpath = "github.com/google/uuid",
    sum = "h1:qJYtXnJRWmpe7m/3XlyhrsLrEURqHRM2kxzoxXqyUDs=",
    version = "v1.2.0",
)

go_repository(
    name = "com_github_google_go_containerregistry",
    importpath = "github.com/google/go-containerregistry",
    sha256 = "cadb09cb5bcbe00688c73d716d1c9e774d6e4959abec4c425a1b995faf33e964",
    strip_prefix = "google-go-containerregistry-8a28419",
    type = "tar.gz",
    urls = ["https://api.github.com/repos/google/go-containerregistry/tarball/8a2841911ffee4f6892ca0083e89752fb46c48dd"],  # v0.1.4
)

# Java.

git_repository(
    name = "com_github_promotedai_schema_internal_git",
    commit = SCHEMA_COMMIT,
    remote = "ssh://git@github.com/promotedai/schema-internal",
)

git_repository(
    name = "com_github_promotedai_schema_aaa_git",
    commit = SCHEMA_aaa_COMMIT,
    remote = "ssh://git@github.com/promotedai/schema-aaa",
)

RULES_JVM_EXTERNAL_TAG = "4.1"

RULES_JVM_EXTERNAL_SHA = "f36441aa876c4f6427bfb2d1f2d723b48e9d930b62662bf723ddfb8fc80f0140"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:defs.bzl", "artifact", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

# When version bumping, please update the "flink" container_pull definition near the bottom of this file.
# Use the correct sha256 for the appropriate scala and flink version defined here.
FLINK_VERSION = "1.14.4"

SCALA_VERSION = "2.12"

AWS_KINESIS_DATA_ANALYTICS_VERSION = "1.1.0"

JACKSON_VERSION = "2.11.2"

HADOOP_VERSION = "3.3.0"

AVRO_VERSION = "1.10.1"

PARQUET_VERSION = "1.12.3"

TEST_CONTAINER_VERSION = "1.17.6"

# TODO - support flink shaded common deps like guava.
# TODO - separate out neverlink.
# https://github.com/bazelbuild/rules_jvm_external
maven_install(
    name = "maven_neverlink",
    artifacts = [
        maven.artifact(
            artifact = "flink-streaming-java_%s" % SCALA_VERSION,
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
            artifact = "flink-clients_%s" % SCALA_VERSION,
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
        maven.artifact(
            artifact = "flink-table-uber_%s" % SCALA_VERSION,
            group = "org.apache.flink",
            neverlink = True,
            version = FLINK_VERSION,
        ),
    ],
    repositories = [
        "https://maven.google.com",
        "https://repo1.maven.org/maven2",
    ],
    # Double the timeout to see if the 504si, that GitHub Actions is seeing, go away.
    resolve_timeout = 1200,
)

maven_install(
    name = "maven_test",
    artifacts = [
        "org.testcontainers:testcontainers:%s" % TEST_CONTAINER_VERSION,
        "org.testcontainers:junit-jupiter:%s" % TEST_CONTAINER_VERSION,
        "org.testcontainers:kafka:%s" % TEST_CONTAINER_VERSION,
    ],
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
        "org.apache.flink:flink-streaming-java_%s:%s" % (SCALA_VERSION, FLINK_VERSION),
        maven.artifact(
            artifact = "flink-streaming-java_%s" % SCALA_VERSION,
            classifier = "tests",
            group = "org.apache.flink",
            version = FLINK_VERSION,
        ),
        "org.apache.flink:flink-streaming-scala_%s:%s" % (SCALA_VERSION, FLINK_VERSION),
        "org.apache.flink:flink-file-sink-common:%s" % FLINK_VERSION,
        "org.apache.flink:flink-csv:%s" % FLINK_VERSION,
        "org.apache.flink:flink-connector-files:%s" % FLINK_VERSION,
        "org.apache.flink:flink-connector-kafka_%s:%s" % (SCALA_VERSION, FLINK_VERSION),
        "org.apache.flink:flink-connector-kinesis_%s:%s" % (SCALA_VERSION, FLINK_VERSION),
        "org.apache.flink:flink-clients_%s:%s" % (SCALA_VERSION, FLINK_VERSION),
        "org.apache.flink:flink-avro:%s" % FLINK_VERSION,
        "org.apache.avro:avro-protobuf:%s" % AVRO_VERSION,
        "org.apache.flink:flink-json:%s" % FLINK_VERSION,
        "ai.promoted:flink-parquet_%s:%s" % (SCALA_VERSION, FLINK_VERSION),
        #"org.apache.flink:flink-parquet_%s:%s" % (SCALA_VERSION, FLINK_VERSION),
        "org.apache.flink:flink-table-common:%s" % FLINK_VERSION,
        maven.artifact(
            artifact = "flink-table-uber_%s" % SCALA_VERSION,
            group = "org.apache.flink",
            version = FLINK_VERSION,
        ),
        "org.apache.flink:flink-test-utils_%s:%s" % (SCALA_VERSION, FLINK_VERSION),
        maven.artifact(
            artifact = "flink-test-utils_%s" % SCALA_VERSION,
            classifier = "tests",
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
        "io.lettuce:lettuce-core:6.1.4.RELEASE",
        "software.amazon.awssdk:bom:2.17.186",
        "software.amazon.awssdk:regions:2.17.186",
        "software.amazon.awssdk:sdk-core:2.17.186",
        "software.amazon.awssdk:secretsmanager:2.17.186",
        "org.lz4:lz4-java:1.8.0",
        # A bunch needed to resolve Hadoop dependencies.
        "org.apache.hadoop:hadoop-aws:2.10.0",
        "org.apache.commons:commons-lang3:3.11",
        "com.github.ben-manes.caffeine:caffeine:2.9.3",
        "com.google.auto.value:auto-value:1.8.2",
        "com.google.auto.value:auto-value-annotations:1.8.2",
        "com.google.guava:guava:31.0.1-jre",
        "com.google.truth:truth:1.1.3",
        "com.google.truth.extensions:truth-java8-extension:1.1.3",
        "com.google.truth.extensions:truth-proto-extension:1.1.3",
        "org.apache.flink:flink-hadoop-compatibility_%s:%s" % (SCALA_VERSION, FLINK_VERSION),
        maven.artifact(
            artifact = "hadoop-client",
            exclusions = [
                maven.exclusion(
                    artifact = "log4j",
                    group = "log4j",
                ),
                "org.slf4j:slf4j-log4j12",
            ],
            group = "org.apache.hadoop",
            version = HADOOP_VERSION,
        ),
        maven.artifact(
            artifact = "hadoop-common",
            exclusions = [
                maven.exclusion(
                    artifact = "log4j",
                    group = "log4j",
                ),
                "org.slf4j:slf4j-log4j12",
            ],
            group = "org.apache.hadoop",
            version = HADOOP_VERSION,
        ),
        "com.twitter:chill-protobuf:0.10.0",
        "com.google.protobuf:protobuf-java:3.15.3",
        "com.twitter:chill_%s:0.10.0" % SCALA_VERSION,
        "com.twitter:chill-java:0.10.0",
        "org.apache.parquet:parquet-avro:%s" % PARQUET_VERSION,
        #"org.apache.parquet:parquet-common:%s" % PARQUET_VERSION,
        #"org.apache.parquet:parquet-hadoop:%s" % PARQUET_VERSION,
        "ai.promoted:parquet-protobuf:%s" % PARQUET_VERSION,
        #"org.apache.parquet:parquet-protobuf:%s" % PARQUET_VERSION,
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
        "org.apache.logging.log4j:log4j-slf4j-impl:2.11.2",
        "org.apache.logging.log4j:log4j-api:2.11.2",
        "org.apache.logging.log4j:log4j-core:2.11.2",
        "org.mockito:mockito-core:3.10.0",
        "org.json:json:20200518",
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
    digest = "sha256:965211e1d53251f26959eb666d4533419608a8681e28c4372dc4326904743346",
    registry = "index.docker.io",
    repository = "library/flink",
    tag = "1.14.4-java11",
)

container_pull(
    name = "lambda_go",
    registry = "lambci",
    repository = "lambda",
    # TODO - use digests instead
    tag = "go1.x",
    # digest = "sha256:deadbeef",
)

go_repository(
    name = "com_github_eapache_go_xerial_snappy",
    importpath = "github.com/eapache/go-xerial-snappy",
    sum = "h1:YEetp8/yCZMuEPMUDHG0CW/brkkEp8mzqk2+ODEitlw=",
    version = "v0.0.0-20180814174437-776d5712da21",
)

go_repository(
    name = "com_github_frankban_quicktest",
    importpath = "github.com/frankban/quicktest",
    sum = "h1:8sXhOn0uLys67V8EsXLc6eszDs8VXWxL3iRvebPhedY=",
    version = "v1.11.3",
)

go_repository(
    name = "com_github_google_go_cmp",
    importpath = "github.com/google/go-cmp",
    sum = "h1:L8R9j+yAqZuZjsqh/z+F1NCffTKKLShY6zXTItVIZ8M=",
    version = "v0.5.4",
)

go_repository(
    name = "com_github_klauspost_compress",
    importpath = "github.com/klauspost/compress",
    sum = "h1:VMAMUUOh+gaxKTMk+zqbjsSjsIcUcL/LF4o63i82QyA=",
    version = "v1.9.8",
)

go_repository(
    name = "com_github_xdg_scram",
    importpath = "github.com/xdg/scram",
    sum = "h1:u40Z8hqBAAQyv+vATcGgV0YCnDjqSL7/q/JyPhhJSPk=",
    version = "v0.0.0-20180814205039-7eeb5667e42c",
)

go_repository(
    name = "com_github_xdg_stringprep",
    importpath = "github.com/xdg/stringprep",
    sum = "h1:d9X0esnoa3dFsV0FG35rAT0RIhYFlPq7MiP+DW89La0=",
    version = "v1.0.0",
)

go_repository(
    name = "in_gopkg_yaml_v3",
    importpath = "gopkg.in/yaml.v3",
    sum = "h1:dUUwHk2QECo/6vqA44rthZ8ie2QXMNeKRTHCNY2nXvo=",
    version = "v3.0.0-20200313102051-9f266ea9e77c",
)

go_repository(
    name = "org_golang_x_xerrors",
    importpath = "golang.org/x/xerrors",
    sum = "h1:E7g+9GITq07hpfrRu66IVDexMakfv52eLZ2CXBWiKr4=",
    version = "v0.0.0-20191204190536-9bdfabe68543",
)

RULES_AVRO_COMMIT = "b112ef1224c6b3eebc52be4d9aa58465487cc05f"

RULES_AVRO_SHA256 = "239f9dc9deecdd8342c9cdb775ce38c2859b3e55b9988b9fc8e4d8e0a1eb5c70"

# Dan reviewed this Tar.  It's small.  Looks safe.
http_archive(
    name = "io_bazel_rules_avro",
    sha256 = RULES_AVRO_SHA256,
    strip_prefix = "rules_avro-%s" % RULES_AVRO_COMMIT,
    url = "https://github.com/chenrui333/rules_avro/archive/%s.tar.gz" % RULES_AVRO_COMMIT,
)

load("@io_bazel_rules_avro//avro:avro.bzl", "avro_repositories")

# avro_repositories()
# or specify a version
avro_repositories(version = AVRO_VERSION)
