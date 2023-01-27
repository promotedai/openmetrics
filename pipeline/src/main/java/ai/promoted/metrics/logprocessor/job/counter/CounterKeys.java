package ai.promoted.metrics.logprocessor.job.counter;

import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink;
import ai.promoted.proto.delivery.internal.features.AggMetric;
import ai.promoted.proto.delivery.internal.features.CountType;
import ai.promoted.proto.delivery.internal.features.CountWindow;
import ai.promoted.proto.event.JoinedEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import ua_parser.Parser;

import java.util.EnumSet;

/** "Interface" used for namespacing keys related to counters. */
interface CounterKeys {
    static final String TYPE_SEPARATOR = Character.toString((char)29);
    static final String USER_TYPE = TYPE_SEPARATOR + "u";
    static final String QUERY_TYPE = TYPE_SEPARATOR + "q";
    static final String ROW_FORMAT_KEY = TYPE_SEPARATOR + RedisSink.JOIN_CHAR + "row_format";
    static final String FEATURE_IDS_KEY = TYPE_SEPARATOR + RedisSink.JOIN_CHAR + "feature_ids";

    static final ImmutableSet<Long> ITEM_DEVICE_COUNT_FIDS = getStandardFeatureIds(CountType.ITEM_DEVICE_COUNT);
    static final ImmutableSet<Long> QUERY_COUNT_FIDS = getStandardFeatureIds(CountType.QUERY_COUNT);
    static final ImmutableSet<Long> ITEM_QUERY_COUNT_FIDS = getStandardFeatureIds(CountType.ITEM_QUERY_COUNT);
    static final ImmutableSet<Long> USER_COUNT_FIDS = getStandardFeatureIds(CountType.USER_COUNT);
    static final ImmutableSet<Long> LOG_USER_COUNT_FIDS = getStandardFeatureIds(CountType.LOG_USER_COUNT);
    static final ImmutableSet<Long> LAST_USER_ITEM_FIDS =
        getLastUserEventFeatureIds(CountType.USER_ITEM_COUNT, CountType.USER_ITEM_HOURS_AGO);
    static final ImmutableSet<Long> LAST_LOG_USER_ITEM_FIDS =
        getLastUserEventFeatureIds(CountType.LOG_USER_ITEM_COUNT, CountType.LOG_USER_ITEM_HOURS_AGO);
    static final ImmutableSet<Long> LAST_USER_QUERY_FIDS =
        getLastUserEventFeatureIds(CountType.USER_QUERY_COUNT, CountType.USER_QUERY_HOURS_AGO);
    static final ImmutableSet<Long> LAST_LOG_USER_QUERY_FIDS =
        getLastUserEventFeatureIds(CountType.LOG_USER_QUERY_COUNT, CountType.LOG_USER_QUERY_HOURS_AGO);

    static final Parser UAParser = new Parser();

    interface CountKey<KEY> extends KeySelector<JoinedEvent, KEY> {
        /** The name of what is being counted.  It's used for logging output paths and operator uid/names. */
        String getName();

        /** The string encoding the output row key and value. */
        String getRowFormat();

        /** The set of feature ids provided by this count key. */
        ImmutableSet<Long> getFeatureIds();

        /** The key's type information */
        TypeInformation<KEY> getTypeInfo();
    }

    /**
     * JoinedEventCountKey defines both the KeySelector to extract the list of dimensions to count
     * and a map function to translate from the Sliding*Counter output to a redis command to sink.
     */
    interface JoinedEventCountKey<KEY> extends CountKey<KEY> {
        /** Maps the the Sliding*Counter output to the redis command to sink. */
        // TODO: remove granularity arg to make this implement the MapFunction interface as well
        RedisSink.Command map(Tuple4<KEY, Integer, Long, Integer> in, String granularity);
    }

    @VisibleForTesting
    static final JoinedEventCountKey<Tuple4<Long, String, String, String>> GLOBAL_EVENT_DEVICE_KEY = new JoinedEventCountKey<>() {
        @Override
        public TypeInformation<Tuple4<Long, String, String, String>> getTypeInfo() {
            return Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.STRING);
        }

        @Override
        public String getName() {
            return "platform-device";
        }

        /** @return platform_id long, event AggMetric(String), os string, user_agent string */
        @Override
        public Tuple4<Long, String, String, String> getKey(JoinedEvent event) {
            String requestAgent = event.getRequest().getDevice().getBrowser().getUserAgent();
            return Tuple4.of(
                    FlatUtil.getPlatformId(event),
                    FlatUtil.getCountAggValue(event).name(),
                    UAParser.parseOS(requestAgent).family,
                    UAParser.parseUserAgent(requestAgent).family);
        }

        /** @return platform_id -> {os, user_agent, fid -> count} */
        @Override
        public RedisSink.Command map(Tuple4<Tuple4<Long, String, String, String>, Integer, Long, Integer> in, String granularity) {
            return RedisSink.hset(
                    Tuple1.of(in.f0.f0),
                    Tuple3.of(in.f0.f2, in.f0.f3, FeatureId.itemDeviceCount(AggMetric.valueOf(in.f0.f1), in.f1, granularity)),
                    in.f2,
                    -1 /* intentionally don't ever expire globals */);
        }

        @Override
        public String getRowFormat() {
            return "os,user_agent,fid:value";
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
            // TODO: accumulate and provide values dynamically
            return ITEM_DEVICE_COUNT_FIDS;
        }
    };

    @VisibleForTesting
    static final JoinedEventCountKey<Tuple5<Long, String, String, String, String>> CONTENT_EVENT_DEVICE_KEY = new JoinedEventCountKey<>() {
        @Override
        public TypeInformation<Tuple5<Long, String, String, String, String>> getTypeInfo() {
            return Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.STRING, Types.STRING);
        }

        @Override
        public String getName() {
            return "content-device";
        }

        /** @return platform_id long, content_id string, event AggMetric(String), os string, user_agent string */
        @Override
        public Tuple5<Long, String, String, String, String> getKey(JoinedEvent event) {
            String requestAgent = event.getRequest().getDevice().getBrowser().getUserAgent();
            return Tuple5.of(
                    FlatUtil.getPlatformId(event),
                    FlatUtil.getContentId(event),
                    FlatUtil.getCountAggValue(event).name(),
                    UAParser.parseOS(requestAgent).family,
                    UAParser.parseUserAgent(requestAgent).family);
        }

        /** @return platform_id, content_id -> {os, user_agent, fid -> count} */
        @Override
        public RedisSink.Command map(Tuple4<Tuple5<Long, String, String, String, String>, Integer, Long, Integer> in, String granularity) {
            return RedisSink.hset(
                    Tuple2.of(in.f0.f0, in.f0.f1),
                    Tuple3.of(in.f0.f3, in.f0.f4, FeatureId.itemDeviceCount(AggMetric.valueOf(in.f0.f2), in.f1, granularity)),
                    in.f2,
                    in.f3);
        }

        @Override
        public String getRowFormat() {
            return "os,user_agent,fid:value";
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
            // TODO: accumulate and provide values dynamically
            return ITEM_DEVICE_COUNT_FIDS;
        }
    };

    interface QueryEventCountKey<KEY> extends JoinedEventCountKey<KEY> {
        /** Static method to extract the query hash value from the key. */
        public long getQueryHash(KEY key);
    };

    @VisibleForTesting
    static final QueryEventCountKey<Tuple3<Long, Long, String>> QUERY_EVENT_KEY = new QueryEventCountKey<>() {
        @Override
        public TypeInformation<Tuple3<Long, Long, String>> getTypeInfo() {
            return Types.TUPLE(Types.LONG, Types.LONG, Types.STRING);
        }

        @Override
        public String getName() {
            return "query";
        }

        /** @return platform_id long, query_hash long, event AggMetric(String) */
        @Override
        public Tuple3<Long, Long, String> getKey(JoinedEvent event) {
            return Tuple3.of(
                    FlatUtil.getPlatformId(event),
                    FlatUtil.getQueryHash(event),
                    FlatUtil.getCountAggValue(event).name());
        }

        @Override
        public long getQueryHash(Tuple3<Long, Long, String> key) {
            return key.f1;
        }

        /** @return platform_id, type, query_hex -> {fid -> count} */
        @Override
        public RedisSink.Command map(Tuple4<Tuple3<Long, Long, String>, Integer, Long, Integer> in, String granularity) {
            return RedisSink.hset(
                    Tuple3.of(in.f0.f0, QUERY_TYPE, Long.toHexString(in.f0.f1)),
                    Tuple1.of(FeatureId.queryCount(AggMetric.valueOf(in.f0.f2), in.f1, granularity)),
                    in.f2,
                    in.f3);
        }

        @Override
        public String getRowFormat() {
            return "fid:value";
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
            // TODO: accumulate and provide values dynamically
            return QUERY_COUNT_FIDS;
        }
    };

    @VisibleForTesting
    static final QueryEventCountKey<Tuple4<Long, Long, String, String>> CONTENT_QUERY_EVENT_KEY = new QueryEventCountKey<>() {
        @Override
        public TypeInformation<Tuple4<Long, Long, String, String>> getTypeInfo() {
            return Types.TUPLE(Types.LONG, Types.LONG, Types.STRING, Types.STRING);
        }

        @Override
        public String getName() {
            return "content-query";
        }

        /** @return platform_id long, query_hash long, content_id string, event AggMetric(String) */
        @Override
        public Tuple4<Long, Long, String, String> getKey(JoinedEvent event) {
            return Tuple4.of(
                    FlatUtil.getPlatformId(event),
                    FlatUtil.getQueryHash(event),
                    FlatUtil.getContentId(event),
                    FlatUtil.getCountAggValue(event).name());
        }

        @Override
        public long getQueryHash(Tuple4<Long, Long, String, String> key) {
            return key.f1;
        }

        /** @return platform_id, content_id, type, query_hex -> {fid -> count} */
        @Override
        public RedisSink.Command map(Tuple4<Tuple4<Long, Long, String, String>, Integer, Long, Integer> in, String granularity) {
            return RedisSink.hset(
                    Tuple4.of(in.f0.f0, in.f0.f2, QUERY_TYPE, Long.toHexString(in.f0.f1)),
                    Tuple1.of(FeatureId.itemQueryCount(AggMetric.valueOf(in.f0.f3), in.f1, granularity)),
                    in.f2,
                    in.f3);
        }

        @Override
        public String getRowFormat() {
            return "fid:value";
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
            // TODO: accumulate and provide values dynamically
            return ITEM_QUERY_COUNT_FIDS;
        }
    };

    @VisibleForTesting
    static abstract class UserEventCountKey implements JoinedEventCountKey<Tuple3<Long, String, String>> {
        private final String name;
        private final boolean isLogUser;

        // Override with getting the luid or uid from the event.
        abstract public String getUserString(JoinedEvent event);

        UserEventCountKey(String name) {
            this.name = name;
            this.isLogUser = name.contains("log");
        }

        @Override
        public TypeInformation<Tuple3<Long, String, String>> getTypeInfo() {
            return Types.TUPLE(Types.LONG, Types.STRING, Types.STRING);
        }

        /** @return platform_id long, user string, event AggMetric(String) */
        @Override
        public Tuple3<Long, String, String> getKey(JoinedEvent event) {
            return Tuple3.of(
                    FlatUtil.getPlatformId(event),
                    getUserString(event),
                    FlatUtil.getCountAggValue(event).name());
        }
        /** @return platform_id, type, user -> {fid -> count} */
        @Override
        public RedisSink.Command map(Tuple4<Tuple3<Long, String, String>, Integer, Long, Integer> in, String granularity) {
            return RedisSink.hset(
                    Tuple3.of(in.f0.f0, USER_TYPE, in.f0.f1),
                    Tuple1.of(FeatureId.userCount(isLogUser(), AggMetric.valueOf(in.f0.f2), in.f1, granularity)),
                    in.f2,
                    in.f3);
        }

        private boolean isLogUser() {
            return isLogUser;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getRowFormat() {
            return "fid:value";
        }
    }

    @VisibleForTesting
    static final UserEventCountKey USER_EVENT_KEY = new UserEventCountKey("user") {
        @Override
        public String getUserString(JoinedEvent event) {
            return FlatUtil.getUserId(event);
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
            // TODO: accumulate and provide values dynamically
            return USER_COUNT_FIDS;
        }
    };

    static final UserEventCountKey LOG_USER_EVENT_KEY = new UserEventCountKey("log-user") {
        @Override
        public String getUserString(JoinedEvent event) {
            return FlatUtil.getLogUserId(event);
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
            // TODO: accumulate and provide values dynamically
            return LOG_USER_COUNT_FIDS;
        }
    };

    static abstract class LastUserEventKey<KEY> implements CountKey<KEY> {
        private final String name;
        private final boolean isLogUser;

        // Override with getting the luid or uid from the event.
        abstract String getUserString(JoinedEvent event);

        abstract RedisSink.Command mapTimestamp(Tuple4<KEY, Long, Long, Integer> in);
        abstract RedisSink.Command mapCount90d(Tuple4<KEY, Long, Long, Integer> in);

        LastUserEventKey(String name) {
            this.name = name;
            this.isLogUser = name.contains("log");
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getRowFormat() {
            return "fid:value";
        }

        protected boolean isLogUser() {
            return isLogUser;
        }
    }

    @VisibleForTesting
    static abstract class LastUserContentKey extends LastUserEventKey<Tuple4<Long, String, String, String>> {
        LastUserContentKey(String name) {
            super(name);
        }

        @Override
        public TypeInformation<Tuple4<Long, String, String, String>> getTypeInfo() {
            return Types.TUPLE(Types.LONG, Types.STRING, Types.STRING, Types.STRING);
        }

        /** @return platform_id long, user string, content_id string, event AggMetric(String) */
        @Override
        public Tuple4<Long, String, String, String> getKey(JoinedEvent event) {
            return Tuple4.of(
                    FlatUtil.getPlatformId(event),
                    getUserString(event),
                    FlatUtil.getContentId(event),
                    FlatUtil.getCountAggValue(event).name());
        }

        /** @return platform_id, type, user, content_id -> {fid -> timestamp} */
        @Override
        public RedisSink.Command mapTimestamp(Tuple4<Tuple4<Long, String, String, String>, Long, Long, Integer> in) {
            return RedisSink.hset(
                    Tuple4.of(in.f0.f0, USER_TYPE, in.f0.f1, in.f0.f2),
                    Tuple1.of(FeatureId.lastUserContentTimestamp(isLogUser(), AggMetric.valueOf(in.f0.f3))),
                    Tuple1.of(in.f1),
                    0);
        }

        /** @return platform_id, type, user, content_id -> {fid -> 90d_count} */
        @Override
        public RedisSink.Command mapCount90d(Tuple4<Tuple4<Long, String, String, String>, Long, Long, Integer> in) {
            return RedisSink.hset(
                    Tuple4.of(in.f0.f0, USER_TYPE, in.f0.f1, in.f0.f2),
                    Tuple1.of(FeatureId.lastUserContentCount(isLogUser(), AggMetric.valueOf(in.f0.f3))),
                    Tuple1.of(in.f2),
                    in.f3);
        }
    }

    @VisibleForTesting
    static final LastUserContentKey LAST_USER_CONTENT_KEY = new LastUserContentKey("last-time-user-event") {
        @Override
        public String getUserString(JoinedEvent event) {
            return FlatUtil.getUserId(event);
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
            // TODO: accumulate and provide values dynamically
            return LAST_USER_ITEM_FIDS;
        }
    };

    static final LastUserContentKey LAST_LOG_USER_CONTENT_KEY = new LastUserContentKey("last-time-log-user-event") {
        @Override
        public String getUserString(JoinedEvent event) {
            return FlatUtil.getLogUserId(event);
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
            // TODO: accumulate and provide values dynamically
            return LAST_LOG_USER_ITEM_FIDS;
        }
    };

    @VisibleForTesting
    static abstract class LastUserQueryKey extends LastUserEventKey<Tuple4<Long, String, Long, String>> {
        LastUserQueryKey(String name) {
            super(name);
        }

        @Override
        public TypeInformation<Tuple4<Long, String, Long, String>> getTypeInfo() {
            return Types.TUPLE(Types.LONG, Types.STRING, Types.LONG, Types.STRING);
        }

        /** @return platform_id long, user string, query_hash long, event AggMetric(String) */
        @Override
        public Tuple4<Long, String, Long, String> getKey(JoinedEvent event) {
            return Tuple4.of(
                    FlatUtil.getPlatformId(event),
                    getUserString(event),
                    FlatUtil.getQueryHash(event),
                    FlatUtil.getCountAggValue(event).name());
        }

        /** Static method to extract the query hash value from the key. */
        public static long getQueryHash(Tuple4<Long, String, Long, String> key) {
            return key.f2;
        }

        /** @return platform_id, type, user, type, query_hex -> {fid -> timestamp} */
        @Override
        public RedisSink.Command mapTimestamp(Tuple4<Tuple4<Long, String, Long, String>, Long, Long, Integer> in) {
            return RedisSink.hset(
                    Tuple5.of(in.f0.f0, USER_TYPE, in.f0.f1, QUERY_TYPE, Long.toHexString(in.f0.f2)),
                    Tuple1.of(FeatureId.lastUserQueryTimestamp(isLogUser(), AggMetric.valueOf(in.f0.f3))),
                    Tuple1.of(in.f1),
                    0);
        }

        /** @return platform_id, type, user, type, query_hex -> {fid -> 90d_count} */
        @Override
        public RedisSink.Command mapCount90d(Tuple4<Tuple4<Long, String, Long, String>, Long, Long, Integer> in) {
            return RedisSink.hset(
                    Tuple5.of(in.f0.f0, USER_TYPE, in.f0.f1, QUERY_TYPE, Long.toHexString(in.f0.f2)),
                    Tuple1.of(FeatureId.lastUserQueryCount(isLogUser(), AggMetric.valueOf(in.f0.f3))),
                    Tuple1.of(in.f2),
                    in.f3);
        }
    }

    @VisibleForTesting
    static final LastUserQueryKey LAST_USER_QUERY_KEY = new LastUserQueryKey("last-time-user-query") {
        @Override
        public String getUserString(JoinedEvent event) {
            return FlatUtil.getUserId(event);
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
            // TODO: accumulate and provide values dynamically
            return LAST_USER_QUERY_FIDS;
        }
    };

    static final LastUserQueryKey LAST_LOG_USER_QUERY_KEY = new LastUserQueryKey("last-time-log-user-query") {
        @Override
        public String getUserString(JoinedEvent event) {
            return FlatUtil.getLogUserId(event);
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
            // TODO: accumulate and provide values dynamically
            return LAST_LOG_USER_QUERY_FIDS;
        }
    };

    private static ImmutableSet<Long> getStandardFeatureIds(CountType type) {
        return FeatureId.expandFeatureIds(
                EnumSet.of(type),
                EnumSet.complementOf(EnumSet.of(AggMetric.UNKNOWN_AGGREGATE, AggMetric.UNRECOGNIZED)),
                EnumSet.of(
                    CountWindow.HOUR,
                    CountWindow.DAY,
                    CountWindow.DAY_7,
                    CountWindow.DAY_30));
    }

    private static ImmutableSet<Long> getLastUserEventFeatureIds(CountType count, CountType hoursAgo) {
        return FeatureId.expandFeatureIds(
                ImmutableList.of(
                    FeatureId.featureId(count, null, CountWindow.DAY_90),
                    FeatureId.featureId(hoursAgo, null, CountWindow.NONE)),
                EnumSet.complementOf(EnumSet.of(AggMetric.UNKNOWN_AGGREGATE, AggMetric.UNRECOGNIZED)));
    }
}
