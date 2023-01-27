package ai.promoted.metrics.logprocessor.common.functions.userjoin;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.common.Properties;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.JoinedIdentifiers;
import ai.promoted.proto.event.User;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.twitter.chill.protobuf.ProtobufSerializer;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UserJoinTest {

    private static final long PLATFORM_ID = 1L;
    private static final String USER_ID = "userId1";
    private static final String LOG_USER_ID = "logUserId1";

    private UserJoin<JoinedEvent> function;
    private KeyedTwoInputStreamOperatorTestHarness harness;

    @BeforeEach
    public void setUp() throws Exception {
        function = new UserJoin<>(
                Duration.ofMillis(-1000),
                Duration.ofMillis(10),
                Duration.ofMillis(100),
                JoinedEvent.class,
                FlatUtil::setFlatUserAndBuild,
                (event) -> event.getTiming().getLogTimestamp(),
                // TODO - implement a very where everything matches so we make sure to execute the code.
                DebugIds.empty(),
                (event) -> false,
                true);
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                function,
                user -> Tuple2.of(user.getPlatformId(), user.getUserInfo().getLogUserId()),
                event -> Tuple2.of(event.getIds().getPlatformId(), event.getIds().getLogUserId()),
                Types.TUPLE(Types.LONG, Types.STRING));
        harness.setTimeCharacteristic(TimeCharacteristic.EventTime);
        harness.getExecutionConfig().registerTypeWithKryoSerializer(
                User.class, ProtobufSerializer.class);
        harness.getExecutionConfig().registerTypeWithKryoSerializer(
                JoinedEvent.class, ProtobufSerializer.class);
    }

    @Test
    public void noInputs() throws Exception {
        harness.processBothWatermarks(new Watermark(2000));
        assertThat(harness.extractOutputValues()).isEmpty();

        // Assert state after cleanup timers.
        assertThat(harness.numEventTimeTimers()).isEqualTo(0);
        // assertNoState fails so don't call it.
    }

    @Test
    public void noEvents() throws Exception {
        User user1 = createUser(100L);
        processElement1(user1);
        harness.processBothWatermarks(new Watermark(100));

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(harness.numEventTimeTimers()).isEqualTo(3);

        harness.processBothWatermarks(new Watermark(1100));
        assertThat(harness.extractOutputValues()).isEmpty();
        assertNoState();
    }

    @Test
    public void noUsers() throws Exception {
        JoinedEvent event1 = createJoinedEvent("imp1", 100L);
        processElement2(event1);
        harness.processBothWatermarks(new Watermark(100));

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(harness.numEventTimeTimers()).isEqualTo(1);

        harness.processBothWatermarks(new Watermark(1100));
        assertThat(harness.extractOutputValues()).isEmpty();
        assertNoState();
    }

    @Test
    public void oneEvent() throws Exception {
        User user1 = createUser(99L);
        processElement1(user1);
        JoinedEvent event1 = createJoinedEvent("imp1", 100L);
        processElement2(event1);

        harness.processBothWatermarks(new Watermark(100));
        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(harness.numEventTimeTimers()).isEqualTo(4);

        harness.processBothWatermarks(new Watermark(106));
        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(harness.numEventTimeTimers()).isEqualTo(4);

        harness.processBothWatermarks(new Watermark(2100));
        assertThat(harness.extractOutputValues())
                .containsExactly(FlatUtil.setFlatUser(event1.toBuilder(), user1, UserJoinTest::noLog).build());
        assertNoState();
    }

    @Test
    public void multipleUsersAndEvents() throws Exception {
        processElement1(createUserUpdate100());
        processElement1(createUserUpdate200());
        processElement1(createUserUpdate300());
        processElement1(createUserUpdate400());

        JoinedEvent event1 = createJoinedEvent("imp1", 100L);
        processElement2(event1);
        harness.processBothWatermarks(new Watermark(109));
        assertThat(harness.extractOutputValues()).isEmpty();
        harness.processBothWatermarks(new Watermark(110));
        User expectedUser1 = createUserBuilder(100L)
                .setUserInfo(createUserUserInfo().toBuilder())
                .build();
        JoinedEvent expectedJoinedEvent1 = FlatUtil.setFlatUser(event1, expectedUser1, UserJoinTest::noLog).build();
        assertThat(harness.extractOutputValues()).containsExactly(expectedJoinedEvent1);
        assertThat(harness.numEventTimeTimers()).isEqualTo(9);

        JoinedEvent event2 = createJoinedEvent("imp2", 200L);
        processElement2(event2);
        harness.processBothWatermarks(new Watermark(209));
        assertThat(harness.extractOutputValues()).hasSize(1);
        harness.processBothWatermarks(new Watermark(210));
        User expectedUser2 = createUserBuilder(200L)
                .setUserInfo(createUserUserInfo().toBuilder().setIsInternalUser(true))
                .setProperties(Properties.newBuilder()
                        .setStruct(Struct.newBuilder()
                                .putFields("key1", Value.newBuilder().setNumberValue(1).build())))
                .build();
        JoinedEvent expectedJoinedEvent2 = FlatUtil.setFlatUser(event2, expectedUser2, UserJoinTest::noLog).build();
        assertThat(harness.extractOutputValues()).hasSize(2);
        assertThat(harness.extractOutputValues().get(1)).isEqualTo(expectedJoinedEvent2);
        assertThat(harness.numEventTimeTimers()).isEqualTo(8);

        // The first 3 user update records get merged together.  Then we output the fast output.
        JoinedEvent event3 = createJoinedEvent("imp3", 300L);
        processElement2(event3);
        harness.processBothWatermarks(new Watermark(309));
        assertThat(harness.extractOutputValues()).hasSize(2);
        harness.processBothWatermarks(new Watermark(310));
        User expectedUser3 = createUserBuilder(300L)
                .setUserInfo(createUserUserInfo().toBuilder().setIsInternalUser(true))
                .setProperties(Properties.newBuilder()
                        .setStruct(Struct.newBuilder()
                                .putFields("key2", Value.newBuilder().setNumberValue(2).build())))
                .build();
        JoinedEvent expectedJoinedEvent3 = FlatUtil.setFlatUser(event3, expectedUser3, UserJoinTest::noLog).build();
        assertThat(harness.extractOutputValues()).hasSize(3);
        assertThat(harness.extractOutputValues().get(2)).isEqualTo(expectedJoinedEvent3);
        assertThat(harness.numEventTimeTimers()).isEqualTo(7);

        // Join another event together.  There's another synthetic user in this time period that updates the timings.
        JoinedEvent event4 = createJoinedEvent("imp4", 400L);
        processElement2(event4);
        harness.processBothWatermarks(new Watermark(409));
        assertThat(harness.extractOutputValues()).hasSize(3);
        harness.processBothWatermarks(new Watermark(410));
        User expectedUser4 = createUserBuilder(400L)
                .setUserInfo(createUserUserInfo().toBuilder().setIsInternalUser(true))
                .setProperties(Properties.newBuilder()
                        .setStruct(Struct.newBuilder()
                                .putFields("key2", Value.newBuilder().setNumberValue(2).build())))
                .build();
        JoinedEvent expectedJoinedEvent4 = FlatUtil.setFlatUser(event4, expectedUser4, UserJoinTest::noLog).build();

        assertThat(harness.extractOutputValues()).hasSize(4);
        assertThat(harness.extractOutputValues().get(3)).isEqualTo(expectedJoinedEvent4);
        assertThat(harness.numEventTimeTimers()).isEqualTo(6);

        harness.processBothWatermarks(new Watermark(1500));
        assertEquals(4, harness.extractOutputValues().size());
        assertNoState();
    }

    // Other test ideas:
    // - We could test out of order events.  The code sorts the events though.

    private User createUserUpdate100() {
        return createUser(100);
    }

    private User createUserUpdate200() {
        User.Builder builder = createUserBuilder(200);
        builder.getUserInfoBuilder().setIsInternalUser(true);
        return builder
                .setProperties(Properties.newBuilder()
                        .setStruct(Struct.newBuilder()
                                .putFields("key1", Value.newBuilder().setNumberValue(1).build())))
                .build();
    }

    private User createUserUpdate300() {
        return createUserBuilder(300L)
                .setProperties(Properties.newBuilder()
                        .setStruct(Struct.newBuilder()
                                .putFields("key2", Value.newBuilder().setNumberValue(2).build())))
                .build();
    }

    private User createUserUpdate400() {
        return createUser(400);
    }

    private static JoinedEvent createJoinedEvent(String impressionId, long logTimestamp) {
        return JoinedEvent.newBuilder()
                .setIds(JoinedIdentifiers.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setLogUserId(LOG_USER_ID)
                        .setImpressionId(impressionId))
                .setTiming(Timing.newBuilder()
                        .setEventApiTimestamp(logTimestamp - 5L)
                        .setLogTimestamp(logTimestamp))
                .setImpression(Impression.newBuilder().setContentId("content1"))
                .build();
    }

    private static User createUser(long logTimestamp) {
        return createUserBuilder(logTimestamp).build();
    }

    private static User.Builder createUserBuilder(long logTimestamp) {
        return createUserBuilder()
                .setTiming(Timing.newBuilder()
                        .setEventApiTimestamp(logTimestamp - 5L)
                        .setLogTimestamp(logTimestamp));
    }

    private static User.Builder createUserBuilder() {
        return User.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(createUserUserInfo());
    }

    private static UserInfo createUserUserInfo() {
        return UserInfo.newBuilder()
                .setUserId(USER_ID)
                .setLogUserId(LOG_USER_ID)
                .build();
    }

    private void processElement1(User user) throws Exception {
        harness.processElement1(user, user.getTiming().getLogTimestamp());
    }

    private void processElement2(JoinedEvent event) throws Exception {
        harness.processElement2(event, event.getTiming().getLogTimestamp());
    }

    private void assertNoState() throws Exception {
        assertEquals(0, harness.numEventTimeTimers());
        assertSize(0, function.users);
        assertTrue(function.fastEvents.isEmpty());
        assertTrue(function.untilEndOfWindowEvents.isEmpty());
    }

    private static void assertSize(int expected, ListState<?> listState) throws Exception {
        assertSize(expected, listState.get());
    }

    private static void assertSize(int expected, Iterable<?> iterable) {
        assertEquals(expected, Iterables.size(iterable));
    }

    private static <T> void assertListEquals(List<T> expected, ListState<T> listState) throws Exception {
        List<T> actual = ImmutableList.copyOf(listState.get());
        assertEquals(expected, actual);
    }

    private static void noLog(OutputTag<MismatchError> tag, MismatchError error) {
        // Do nothing.
    }
}
