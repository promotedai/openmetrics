package ai.promoted.metrics.logprocessor.job.raw;

import ai.promoted.metrics.common.DeliveryLogIds;
import ai.promoted.metrics.common.ExecutionServer;
import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.common.RequestInsertionIds;
import ai.promoted.metrics.common.ResponseInsertionIds;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestFactory;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.LogRequestIterator;
import ai.promoted.metrics.logprocessor.common.job.testing.BaseJobMiniclusterTest;
import ai.promoted.metrics.logprocessor.common.job.testing.FlinkJobTerminateHelperException;
import ai.promoted.metrics.logprocessor.common.testing.MiniClusterExtension;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.LogRequest;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.time.Duration;
import java.util.List;

import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.assertAvroFiles;
import static ai.promoted.metrics.logprocessor.common.testing.AvroAsserts.assertAvroParquetFiles;
import static ai.promoted.metrics.logprocessor.common.testing.AvroProtoUtils.toFixedAvroGenericRecords;
import static ai.promoted.metrics.logprocessor.common.testing.FileAsserts.assertPartFilesExist;

/**
 * Warning: this test runs the stream jobs as batch.
 * Flink stream minicluster tests don't automatically checkpoint at the end.
 * TODO - add link to thread about this.
 */
@ExtendWith(MiniClusterExtension.class)
@Testcontainers
public class RawOutputJobMiniclusterTest extends BaseJobMiniclusterTest<RawOutputJob> {
    @Override
    protected RawOutputJob createJob() {
        RawOutputJob job = new RawOutputJob();
        job.s3FileOutput.s3OutputDirectory = tempDir.getAbsolutePath();
        // Checkpoint more frequently so we don't hit part files.
        job.checkpointInterval = Duration.ofSeconds(1);
        job.checkpointTimeout = Duration.ofSeconds(1);
        job.configureExecutionEnvironment(env, 1, 0);
        return job;
    }

    /**
     * Test writing/reading LogUserUser events to/from Kafka.
     */
    @Test
    void testLogLogUserToKafka() throws Exception {
        // TODO This is a tricky test since we can't properly fetch bounded events from Kafka with the current
        //  implementation. Refactor this after we switch Flink version to > 1.14.4 and replace KafkaConsumer with
        //  KafkaConnector.
        try (KafkaContainer kafkaContainer = new KafkaContainer(
                DockerImageName.parse(KAFKA_CONTAINER_IMAGE))) {
            kafkaContainer.start();

            List<LogRequest> logRequests = LogRequestFactory.createLogRequests(1601596151000L,
                    LogRequestFactory.DetailLevel.PARTIAL, 1);
            RawOutputJob job = createJob();
            job.writeLogUserUserEventsToKafka = true;
            job.jobLabel = "blue";
            job.kafkaSegment.bootstrapServers = kafkaContainer.getBootstrapServers();
            job.kafkaSegment.startFromEarliest = true;

            // Manually set the log stream underneath for processing.
            job.outputLogRequest(job.metricsApiKafkaSource.splitSources(fromItems(env, "logRequest", logRequests,
                    LogRequest::getTiming)));
            waitForDone(env.execute("log-log-request"));

            // Setup another job to read from the topic and compare results.
            env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
            job.rawOutputKafka.getLogUserUserSource(
                    env,
                    "test-group",
                    job.rawOutputKafka.getLogUserUserTopic(job.getJobLabel())).addSink(new SinkFunction<>() {
                @Override
                public void invoke(LogUserUser value, Context context) throws Exception {
                    Assertions.assertEquals(value.getUserId(), "userId1");
                    Assertions.assertEquals(value.getLogUserId(), "00000000-0000-0000-0000-000000000001");
                    throw new FlinkJobTerminateHelperException("help-to-break");
                }
            }).uid("Collection-Sink");
            try {
                waitForDone(env.execute("log-log-user-consume"));
            } catch (RuntimeException ex) {
                if (ex.getMessage().contains("org.opentest4j.AssertionFailedError") ||
                        !ex.getMessage().contains("help-to-break")) {
                    throw ex;
                }
            }
        }
    }

    @Test
    void testSimpleCase() throws Exception {
        List<LogRequest> logRequests = LogRequestFactory.createLogRequests(1601596151000L,
                LogRequestFactory.DetailLevel.PARTIAL, 1);

        assertOutputSplitLogRequestTest(
                logRequests,
                logRequests,
                LogRequestFactory.DetailLevel.PARTIAL,
                1601596151000L,
                "2020-10-01",
                "23");
    }

    @Test
    void testDuplicates() throws Exception {
        List<LogRequest> logRequests = LogRequestFactory.createLogRequests(1601603351000L,
                LogRequestFactory.DetailLevel.FULL, 1);
        ImmutableList.Builder<LogRequest> builder = ImmutableList.builder();
        builder.addAll(logRequests);
        for (LogRequest logRequest : logRequests) {
            if (logRequest.getDeliveryLogCount() > 0
                    || logRequest.getCohortMembershipCount() > 0
                    || logRequest.getViewCount() > 0
                    || logRequest.getAutoViewCount() > 0
                    || logRequest.getImpressionCount() > 0
                    || logRequest.getActionCount() > 0) {
                builder.add(logRequest);
            }
        }

        assertOutputSplitLogRequestTest(
                builder.build(),
                logRequests,
                LogRequestFactory.DetailLevel.FULL,
                1601603351000L,
                "2020-10-02",
                "01");
    }

    @Test
    void testInsertionMatrixConversion() throws Exception {
        List<LogRequest> inputLogRequests = ImmutableList.copyOf(new LogRequestIterator(
                LogRequestFactory.createLogRequestOptionsBuilder(1601603351000L, LogRequestFactory.DetailLevel.FULL, 1)
                        .setInsertionMatrixFormat(true)
                        .setInsertionFullFormat(false)));
        List<LogRequest> expectedLogRequests = ImmutableList.copyOf(new LogRequestIterator(
                LogRequestFactory.createLogRequestOptionsBuilder(1601603351000L, LogRequestFactory.DetailLevel.FULL, 1)
                        .setInsertionMatrixFormat(false)
                        .setInsertionFullFormat(true)));

        String dt = "2020-10-02";
        String hour = "01";
        RawOutputJob job = createJob();
        job.jobLabel = "blue";

        // Manually set the log stream underneath for processing.
        job.outputLogRequest(job.metricsApiKafkaSource.splitSources(fromItems(env, "logRequest", inputLogRequests,
                LogRequest::getTiming)));
        waitForDone(env.execute("log-log-request"));

        String path = String.format("blue/raw/log-request/dt=%s/hour=%s", dt, hour);
        File[] avroFiles = assertPartFilesExist(new File(tempDir, path));
        // We don't restructure the input LogRequest records.
        assertAvroFiles(toFixedAvroGenericRecords(inputLogRequests), avroFiles, "raw/log-request");

        path = String.format("blue/raw/delivery-log/dt=%s/hour=%s", dt, hour);
        avroFiles = assertPartFilesExist(new File(tempDir, path));
        assertAvroFiles(
                toFixedAvroGenericRecords(LogRequestFactory.pushDownToDeliveryLogs(expectedLogRequests).stream().map(
                        deliveryLog -> {
                            DeliveryLog.Builder builder = deliveryLog.toBuilder();
                            builder.getRequestBuilder().getUserInfoBuilder().clearUserId();
                            return builder.build();
                        })),
                avroFiles, "raw/delivery-log");
    }

    private void assertOutputSplitLogRequestTest(
            List<LogRequest> inputLogRequests,
            List<LogRequest> expectedLogRequests,
            LogRequestFactory.DetailLevel detailLevel,
            long timeMillis,
            String dt,
            String hour) throws Exception {
        RawOutputJob job = createJob();
        job.jobLabel = "blue";

        // Manually set the log stream underneath for processing.
        job.outputLogRequest(job.metricsApiKafkaSource.splitSources(fromItems(env, "logRequest", inputLogRequests,
                LogRequest::getTiming)));
        waitForDone(env.execute("log-log-request"));

        String path = String.format("blue/raw/log-request/dt=%s/hour=%s", dt, hour);
        File[] avroFiles = assertPartFilesExist(new File(tempDir, path));
        assertAvroFiles(toFixedAvroGenericRecords(expectedLogRequests), avroFiles, "raw/log-request");

        path = String.format("blue/raw/user/dt=%s/hour=%s", dt, hour);
        avroFiles = assertPartFilesExist(new File(tempDir, path));
        assertAvroFiles(
                toFixedAvroGenericRecords(LogRequestFactory.pushDownToUsers(expectedLogRequests)),
                avroFiles, "raw/user");

        path = String.format("blue/raw/delivery-log/dt=%s/hour=%s", dt, hour);
        avroFiles = assertPartFilesExist(new File(tempDir, path));
        assertAvroFiles(
                toFixedAvroGenericRecords(LogRequestFactory.pushDownToDeliveryLogs(expectedLogRequests).stream().map(
                        deliveryLog -> {
                            DeliveryLog.Builder builder = deliveryLog.toBuilder();
                            builder.getRequestBuilder().getUserInfoBuilder().clearUserId();
                            return builder.build();
                        })),
                avroFiles, "raw/delivery-log");

        path = String.format("blue/raw/impression/dt=%s/hour=%s", dt, hour);
        avroFiles = assertPartFilesExist(new File(tempDir, path));
        assertAvroFiles(
                toFixedAvroGenericRecords(LogRequestFactory.pushDownToImpressions(expectedLogRequests)),
                avroFiles, "raw/impression");

        path = String.format("blue/raw/action/dt=%s/hour=%s", dt, hour);
        avroFiles = assertPartFilesExist(new File(tempDir, path));
        assertAvroFiles(
                toFixedAvroGenericRecords(LogRequestFactory.pushDownToActions(expectedLogRequests)),
                avroFiles, "raw/action");

        path = String.format("blue/raw/cohort-membership/dt=%s/hour=%s", dt, hour);
        avroFiles = assertPartFilesExist(new File(tempDir, path));
        assertAvroFiles(
                toFixedAvroGenericRecords(LogRequestFactory.pushDownToCohortMemberships(expectedLogRequests)),
                avroFiles, "raw/cohort-membership");

        path = String.format("blue/raw/view/dt=%s/hour=%s", dt, hour);
        avroFiles = assertPartFilesExist(new File(tempDir, path));
        assertAvroFiles(
                toFixedAvroGenericRecords(LogRequestFactory.pushDownToViews(expectedLogRequests)),
                avroFiles, "raw/view");

        path = String.format("blue/raw/auto-view/dt=%s/hour=%s", dt, hour);
        avroFiles = assertPartFilesExist(new File(tempDir, path));
        assertAvroFiles(
                toFixedAvroGenericRecords(LogRequestFactory.pushDownToAutoViews(expectedLogRequests)),
                avroFiles, "raw/auto-view");

        path = String.format("blue/raw/diagnostics/dt=%s/hour=%s", dt, hour);
        avroFiles = assertPartFilesExist(new File(tempDir, path));
        assertAvroFiles(
                toFixedAvroGenericRecords(LogRequestFactory.pushDownToDiagnostics(expectedLogRequests)),
                avroFiles, "raw/diagnostics");

        path = String.format("blue/raw/log-user-user/dt=%s/hour=%s", dt, hour);
        File[] parquetFiles = assertPartFilesExist(new File(tempDir, path));
        LogRequest logRequest = expectedLogRequests.get(0);
        assertAvroParquetFiles(
                LogUserUser.newBuilder()
                        .setPlatformId(logRequest.getPlatformId())
                        .setLogUserId(logRequest.getUserInfo().getLogUserId())
                        .setUserId(logRequest.getUserInfo().getUserId())
                        .setEventApiTimestamp(timeMillis)
                        .build(),
                parquetFiles, "blue/raw/log-user-user");

        path = String.format("blue/raw-side/delivery-log-ids/dt=%s/hour=%s", dt, hour);
        parquetFiles = assertPartFilesExist(new File(tempDir, path));
        boolean hasFullDetails = detailLevel == LogRequestFactory.DetailLevel.FULL;
        assertAvroParquetFiles(
                ImmutableList.of(
                        DeliveryLogIds.newBuilder()
                                .setPlatformId(logRequest.getPlatformId())
                                .setLogUserId(logRequest.getUserInfo().getLogUserId())
                                .setEventApiTimestamp(timeMillis)
                                .setExecutionServer(ExecutionServer.SDK)
                                .setViewId("22222222-2222-2222-0000-000000000001")
                                .setRequestId("33333333-3333-3333-0000-000000000001")
                                .setClientRequestId("client-33333333-3333-3333-0000-000000000001")
                                .setSearchQuery(hasFullDetails ? "prom dresses" : "")
                                .setUserAgent(hasFullDetails ? "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " +
                                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36" :
                                        "")
                                .build()),
                parquetFiles, "blue/raw-side/delivery-log-ids");

        path = String.format("blue/raw-side/request-insertion-ids/dt=%s/hour=%s", dt, hour);
        parquetFiles = assertPartFilesExist(new File(tempDir, path));
        assertAvroParquetFiles(
                ImmutableList.of(
                        RequestInsertionIds.newBuilder()
                                .setPlatformId(logRequest.getPlatformId())
                                .setEventApiTimestamp(timeMillis)
                                .setRequestId("33333333-3333-3333-0000-000000000001")
                                .setContentId("i-1-1")
                                .setRetrievalRank(null)
                                .build()),
                parquetFiles, "blue/raw-side/request-insertion-ids");

        path = String.format("blue/raw-side/response-insertion-ids/dt=%s/hour=%s", dt, hour);
        parquetFiles = assertPartFilesExist(new File(tempDir, path));
        assertAvroParquetFiles(
                ImmutableList.of(
                        ResponseInsertionIds.newBuilder()
                                .setPlatformId(logRequest.getPlatformId())
                                .setEventApiTimestamp(timeMillis)
                                .setRequestId("33333333-3333-3333-0000-000000000001")
                                .setInsertionId("44444444-4444-4444-0000-000000000001")
                                .setContentId("i-1-1")
                                .setPosition(0L)
                                .build()),
                parquetFiles, "blue/raw-side/request-insertion-ids");
    }
}
