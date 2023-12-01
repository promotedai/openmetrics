# Suffixed Filesystem

The default Flink Table `filesystem` connector does not support file suffixes (e.g. `.csv`).
This package was forked from `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/filesystem` and has a
very small addition to add suffixes.

Also, it allows us to change some behaviours of the `filesystem` connector.
We extended it to allow setting an extra Avro schema when reading Avro files with Flink SQL.

## How is this format installed?

Flink requires extra formats to be installed using `Services`. A `BUILD` target creates a jar
with `META-INF/services/org.apache.flink.table.factories.Factory` that registers this format. Bazel build will merge the
jars together.

## How different is this from the built-in csv format?

Most of the code is not modified. This fork modifies the original:

- Strips out some sink code.
- Adds a `SuffixedFileSystemConnectorOptions.SUFFIX_NAME`.

## Automated tests

The ContentMetrics job runs a minicluster test. This isn't great but this will catch important problems.

If we do more modifications to the code, we can copy over the tests.

## Future challenges

We'll probably need to refork this package when we upgrade Flink versions.
