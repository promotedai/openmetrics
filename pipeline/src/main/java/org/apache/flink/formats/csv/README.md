# Header CSV

The default Flink Table `csv` format does not include a csv headers.
This package was forked
from [org.apache.flink.formats.csv](https://github.com/apache/flink/tree/release-1.16/flink-formats/flink-csv/src/main/java/org/apache/flink/formats/csv)
and has a small hack to output a csv header.

## How is this format installed?

Flink requires extra formats to be installed using `Services`.  A `BUILD` target creates a jar with `META-INF/services/org.apache.flink.table.factories.Factory` that registers this format.  Bazel build will merge the jars together.

## How different is this from the built-in csv format?

Most of the code is not modified.  This fork modifies the original:
- strips out the input version (not used).
- Switch CsvRowDataSerializationSchema (an Encoder) to CsvRowDataBulkWriter (BulkWriter).  This allows us to control the creation of the file (which the Encoder does not support). 

## Automated tests

Dan tried to copy the automated tests but most of them require that we keep the csv deserialization code.  We don't want to support that.

The ContentMetrics job runs a minicluster test.  This isn't great but this will catch important problems.

If we do more modifications to the code, we can copy over the tests.

## Future challenges

We'll probably need to refork this package when we upgrade Flink versions.
