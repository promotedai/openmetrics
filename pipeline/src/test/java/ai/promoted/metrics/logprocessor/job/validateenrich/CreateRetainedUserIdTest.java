package ai.promoted.metrics.logprocessor.job.validateenrich;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.metrics.logprocessor.common.util.TrackingUtil;
import ai.promoted.proto.common.RetainedUser;
import ai.promoted.proto.common.RetainedUserState;
import java.time.Instant;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CreateRetainedUserIdTest {
  private CreateRetainedUserId fn;
  private KeyedOneInputStreamOperatorTestHarness<
          AuthUserIdKey, RetainedUserLookupRow, SelectRetainedUserChange>
      harness;

  @BeforeEach
  public void setUp() throws Exception {
    RetainedUserIdAnonymizer anonymizer =
        new RetainedUserIdAnonymizer(null) {
          final int i = 0;

          @Override
          public String apply(RetainedUserState state, long eventTime) {
            return "uid" + i;
          }
        };
    fn = new CreateRetainedUserId(anonymizer, 10L);
    harness =
        ProcessFunctionTestHarnesses.forKeyedProcessFunction(
            fn, RetainedUserLookupRow::toKey, Types.TUPLE(Types.LONG, Types.STRING));
    TrackingUtil.processingTimeSupplier = () -> 0L;
  }

  @AfterEach
  public void tearDown() throws Exception {
    assertThat(harness.numEventTimeTimers()).isEqualTo(0);

    assertThat(fn.retainedUser.value()).isNull();
    assertThat(fn.outputTime.value()).isNull();
    assertThat(fn.cleanUpTime.value()).isNull();
  }

  @Test
  public void oneRow() throws Exception {
    harness.processElement(
        new RetainedUserLookupRow(
            1L, "userId", "anonUserId", "retainedUserId", null, Instant.ofEpochMilli(2)),
        2L);
    harness.processWatermark(new Watermark(2L));
    assertThat(harness.extractOutputValues())
        .containsExactly(
            new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "retainedUserId"));
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processWatermark(new Watermark(12L));
    assertThat(harness.extractOutputValues())
        .containsExactly(
            new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "retainedUserId"),
            new SelectRetainedUserChange(ChangeKind.DELETE, 1L, "userId", "retainedUserId"));
    assertThat(harness.getSideOutput(CreateRetainedUserId.NEW_RETAINED_USERS_TAG)).isNull();
  }

  @Test
  public void noRetainedUserId() throws Exception {
    harness.processElement(
        new RetainedUserLookupRow(1L, "userId", "anonUserId", null, null, Instant.ofEpochMilli(2)),
        2L);
    harness.processWatermark(new Watermark(2L));
    // Uses the anonUserId as the retainedUserId.
    assertThat(harness.extractOutputValues())
        .containsExactly(
            new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "anonUserId"));
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processWatermark(new Watermark(12L));
    assertThat(harness.extractOutputValues())
        .containsExactly(
            new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "anonUserId"),
            new SelectRetainedUserChange(ChangeKind.DELETE, 1L, "userId", "anonUserId"));
    assertThat(harness.getSideOutput(CreateRetainedUserId.NEW_RETAINED_USERS_TAG))
        .containsExactly(
            new StreamRecord(
                RetainedUser.newBuilder()
                    .setPlatformId(1L)
                    .setUserId("userId")
                    .setRetainedUserId("anonUserId")
                    .setCreateEventApiTimeMillis(2L)
                    .build(),
                2));
  }

  @Test
  public void noRetainedUserId_noAnonUserId() throws Exception {
    harness.processElement(
        new RetainedUserLookupRow(1L, "userId", "", null, null, Instant.ofEpochMilli(2)), 2L);
    harness.processWatermark(new Watermark(2L));
    // Uses the UID generator.
    assertThat(harness.extractOutputValues())
        .containsExactly(new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "uid0"));
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processWatermark(new Watermark(12L));
    assertThat(harness.extractOutputValues())
        .containsExactly(
            new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "uid0"),
            new SelectRetainedUserChange(ChangeKind.DELETE, 1L, "userId", "uid0"));
    assertThat(harness.getSideOutput(CreateRetainedUserId.NEW_RETAINED_USERS_TAG))
        .containsExactly(
            new StreamRecord(
                RetainedUser.newBuilder()
                    .setPlatformId(1L)
                    .setUserId("userId")
                    .setRetainedUserId("uid0")
                    .setCreateEventApiTimeMillis(2L)
                    .build(),
                2));
  }

  @Test
  public void rowsAfterFirstTimerIgnored() throws Exception {
    harness.processElement(
        new RetainedUserLookupRow(1L, "userId", "", null, null, Instant.ofEpochMilli(2)), 2L);
    harness.processWatermark(new Watermark(2L));
    harness.processElement(
        new RetainedUserLookupRow(1L, "userId", "anonUserId", null, null, Instant.ofEpochMilli(3)),
        3L);
    harness.processElement(
        new RetainedUserLookupRow(1L, "userId", "anonUserId2", null, null, Instant.ofEpochMilli(4)),
        4L);
    // Uses the UID generator.
    assertThat(harness.extractOutputValues())
        .containsExactly(new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "uid0"));
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processWatermark(new Watermark(14L));
    assertThat(harness.extractOutputValues())
        .containsExactly(
            new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "uid0"),
            new SelectRetainedUserChange(ChangeKind.DELETE, 1L, "userId", "uid0"));
    assertThat(harness.extractOutputValues()).hasSize(2);
    assertThat(harness.getSideOutput(CreateRetainedUserId.NEW_RETAINED_USERS_TAG))
        .containsExactly(
            new StreamRecord(
                RetainedUser.newBuilder()
                    .setPlatformId(1L)
                    .setUserId("userId")
                    .setRetainedUserId("uid0")
                    .setCreateEventApiTimeMillis(2L)
                    .build(),
                2));
  }

  @Test
  public void useEarlierRow() throws Exception {
    harness.processElement(
        new RetainedUserLookupRow(1L, "userId", "anonUserId2", null, null, Instant.ofEpochMilli(2)),
        2L);
    harness.processElement(
        new RetainedUserLookupRow(1L, "userId", "anonUserId1", null, null, Instant.ofEpochMilli(1)),
        1L);
    harness.processWatermark(new Watermark(2L));
    assertThat(harness.extractOutputValues())
        .containsExactly(
            new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "anonUserId1"));
    assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    harness.processWatermark(new Watermark(12L));
    assertThat(harness.extractOutputValues())
        .containsExactly(
            new SelectRetainedUserChange(ChangeKind.UPSERT, 1L, "userId", "anonUserId1"),
            new SelectRetainedUserChange(ChangeKind.DELETE, 1L, "userId", "anonUserId1"));
    assertThat(harness.extractOutputValues()).hasSize(2);
    assertThat(harness.getSideOutput(CreateRetainedUserId.NEW_RETAINED_USERS_TAG))
        .containsExactly(
            new StreamRecord(
                RetainedUser.newBuilder()
                    .setPlatformId(1L)
                    .setUserId("userId")
                    .setRetainedUserId("anonUserId1")
                    .setCreateEventApiTimeMillis(1L)
                    .build(),
                1));
  }
}
