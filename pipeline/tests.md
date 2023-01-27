# Tests

The Flink pipeline has
- Simple unit tests.
- Simple Flink-level tests for the batch and stream jobs.  These mock our the source and sinks.

We do not have integration tests.  We'll add them later.

# Example commands

`ibazel` is a way to run bazel as a watcher.  Tests will get re-run automatically.

```
ibazel test --test_output=errors src/test/java/ai/promoted/metrics/logprocessor/...

bazel test --test_output=summary --cache_test_results=no src/test/java/ai/promoted/metrics/logprocessor/...
```

# Flink tests

## Advice

- Some `insertSQL` calls do not execute immediately.  You need to perform a wait statement so the jobs can finish.

- If you are hitting weird issues, change `//pipeline/src/resources/log4j-test.xml`.

- If you are hitting weird issues, run the test in a debugger.  IntelliJ has a good Bazel plugin.  Add `fetch_sources = True,` to `maven_install` in `WORKSPACE` to make the packages easier to step through.

## Libraries

Flink has a bunch of test libraries that we can reuse.  It's hard to discover the libraries that are available.  Here are some.

https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/testing.html#junit-rule-miniclusterwithclientresource

https://lists.apache.org/thread.html/rfe3b45a10fc58cf19d2f71c6841515eb7175ba731d5055b06f236b3f%40%3Cuser.flink.apache.org%3E

https://github.com/apache/flink/blob/release-1.11/flink-table/flink-table-planner-blink/src/test/java/org/apache/flink/table/planner/runtime/stream/sql/FunctionITCase.java

# Test Coverage

Warning: a lot of the Flink business logic is handled in SQL statements.  We do not have a code coverage setup for this.  Our current code coverage is mostly around surrounding code.

Install genhtml.

```
brew install lcov
```

Run coverage report.

```
bazel coverage --combined_report=lcov --nocache_test_results src/test/java/ai/promoted/metrics/logprocessor/...
```

Take the combined `coverage.dat`.  You need to run `genhtml` at the WORKSPACE root.

```
genhtml -o /tmp/metricscoverage /private/var/tmp/_bazel_quietgolfer/26a3109a26fa69593e2da03891176bee/execroot/event/bazel-out/_coverage/_coverage_report.dat
```
