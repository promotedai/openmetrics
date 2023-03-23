package ai.promoted.metrics.logprocessor.job.counter;

import static ai.promoted.metrics.logprocessor.common.counter.Constants.TYPE_SEPARATOR;

import ai.promoted.metrics.logprocessor.common.counter.ContentEventDevice;
import ai.promoted.metrics.logprocessor.common.counter.ContentQueryEvent;
import ai.promoted.metrics.logprocessor.common.counter.FeatureId;
import ai.promoted.metrics.logprocessor.common.counter.GlobalEventDevice;
import ai.promoted.metrics.logprocessor.common.counter.JoinedEventRedisHashSupplier;
import ai.promoted.metrics.logprocessor.common.counter.LastLogUserContent;
import ai.promoted.metrics.logprocessor.common.counter.LastLogUserQuery;
import ai.promoted.metrics.logprocessor.common.counter.LastTimeAggResult;
import ai.promoted.metrics.logprocessor.common.counter.LastUserContent;
import ai.promoted.metrics.logprocessor.common.counter.LastUserEventRedisHashSupplier;
import ai.promoted.metrics.logprocessor.common.counter.LastUserQuery;
import ai.promoted.metrics.logprocessor.common.counter.LogUserEvent;
import ai.promoted.metrics.logprocessor.common.counter.QueryEvent;
import ai.promoted.metrics.logprocessor.common.counter.UserEvent;
import ai.promoted.metrics.logprocessor.common.counter.WindowAggResult;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink.Command;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.delivery.internal.features.AggMetric;
import ai.promoted.proto.delivery.internal.features.CountType;
import ai.promoted.proto.delivery.internal.features.CountWindow;
import ai.promoted.proto.event.JoinedEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.EnumSet;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import ua_parser.Parser;

/** "Interface" used for namespacing keys related to counters. */
public interface CounterKeys {
  String ROW_FORMAT_KEY = TYPE_SEPARATOR + RedisSink.JOIN_CHAR + "row_format";
  String FEATURE_IDS_KEY = TYPE_SEPARATOR + RedisSink.JOIN_CHAR + "feature_ids";
  Parser UAParser = new Parser();

  @VisibleForTesting
  QueryEventCountKey<QueryEvent> QUERY_EVENT_KEY =
      new QueryEventCountKey<>(QueryEvent.NAME) {
        @Override
        public TypeInformation<QueryEvent> getTypeInfo() {
          return TypeInformation.of(QueryEvent.class);
        }

        @Override
        public QueryEvent getKey(JoinedEvent event) {
          return new QueryEvent(
              FlatUtil.getPlatformId(event),
              FlatUtil.getQueryHash(event),
              FlatUtil.getAggMetricValue(event).name());
        }

        @Override
        public long getQueryHash(QueryEvent key) {
          return key.getQueryHash();
        }

        /**
         * @return platform_id, type, query_hex -> {fid -> count}
         */
        @Override
        public RedisSink.Command map(WindowAggResult<QueryEvent> in) {
          return RedisSink.hsetOrDel(
              in.getKey().getHashKey(),
              in.getKey().getHashField(in.getWindowSize(), in.getWindowUnit()),
              in.getCount(),
              in.getTtl());
        }

        @Override
        public String getRowFormat() {
          return "fid:value";
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
          // TODO: accumulate and provide values dynamically
          return getStandardFeatureIds(CountType.QUERY_COUNT);
        }
      };

  @VisibleForTesting
  QueryEventCountKey<ContentQueryEvent> CONTENT_QUERY_EVENT_KEY =
      new QueryEventCountKey<>(ContentQueryEvent.NAME) {
        @Override
        public TypeInformation<ContentQueryEvent> getTypeInfo() {
          return TypeInformation.of(ContentQueryEvent.class);
        }

        @Override
        public ContentQueryEvent getKey(JoinedEvent event) {
          return new ContentQueryEvent(
              FlatUtil.getPlatformId(event),
              FlatUtil.getQueryHash(event),
              FlatUtil.getContentId(event),
              FlatUtil.getAggMetricValue(event).name());
        }

        @Override
        public long getQueryHash(ContentQueryEvent key) {
          return key.getQueryHash();
        }

        /**
         * @return platform_id, content_id, type, query_hex -> {fid -> count}
         */
        @Override
        public RedisSink.Command map(WindowAggResult<ContentQueryEvent> in) {
          return RedisSink.hsetOrDel(
              in.getKey().getHashKey(),
              in.getKey().getHashField(in.getWindowSize(), in.getWindowUnit()),
              in.getCount(),
              in.getTtl());
        }

        @Override
        public String getRowFormat() {
          return "fid:value";
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
          // TODO: accumulate and provide values dynamically
          return getStandardFeatureIds(CountType.ITEM_QUERY_COUNT);
        }
      };

  @VisibleForTesting
  JoinedEventCountKey<GlobalEventDevice> GLOBAL_EVENT_DEVICE_KEY =
      new JoinedEventCountKey<>(GlobalEventDevice.NAME) {
        @Override
        public TypeInformation<GlobalEventDevice> getTypeInfo() {
          return TypeInformation.of(GlobalEventDevice.class);
        }

        @Override
        public GlobalEventDevice getKey(JoinedEvent event) {
          String requestAgent = event.getRequest().getDevice().getBrowser().getUserAgent();
          return new GlobalEventDevice(
              FlatUtil.getPlatformId(event),
              FlatUtil.getAggMetricValue(event).name(),
              UAParser.parseOS(requestAgent).family,
              UAParser.parseUserAgent(requestAgent).family);
        }

        @Override
        public RedisSink.Command map(WindowAggResult<GlobalEventDevice> in) {
          return RedisSink.hsetOrDel(
              in.getKey().getHashKey(),
              in.getKey().getHashField(in.getWindowSize(), in.getWindowUnit()),
              in.getCount(),
              -1 /* intentionally don't ever expire globals */);
        }

        @Override
        public String getRowFormat() {
          return "os,user_agent,fid:value";
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
          return getStandardFeatureIds(CountType.ITEM_DEVICE_COUNT);
        }
      };

  @VisibleForTesting
  JoinedEventCountKey<ContentEventDevice> CONTENT_EVENT_DEVICE_KEY =
      new JoinedEventCountKey<>(ContentEventDevice.NAME) {
        @Override
        public TypeInformation<ContentEventDevice> getTypeInfo() {
          return TypeInformation.of(ContentEventDevice.class);
        }

        @Override
        public ContentEventDevice getKey(JoinedEvent event) {
          String requestAgent = event.getRequest().getDevice().getBrowser().getUserAgent();
          return new ContentEventDevice(
              FlatUtil.getPlatformId(event),
              FlatUtil.getContentId(event),
              FlatUtil.getAggMetricValue(event).name(),
              UAParser.parseOS(requestAgent).family,
              UAParser.parseUserAgent(requestAgent).family);
        }

        @Override
        public RedisSink.Command map(WindowAggResult<ContentEventDevice> in) {
          return RedisSink.hsetOrDel(
              in.getKey().getHashKey(),
              in.getKey().getHashField(in.getWindowSize(), in.getWindowUnit()),
              in.getCount(),
              in.getTtl());
        }

        @Override
        public String getRowFormat() {
          return "os,user_agent,fid:value";
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
          return getStandardFeatureIds(CountType.ITEM_DEVICE_COUNT);
        }
      };

  @VisibleForTesting
  UserEventCountKey<UserEvent> USER_EVENT_KEY =
      new UserEventCountKey<>(UserEvent.NAME) {

        @Override
        public UserEvent getKey(JoinedEvent event) throws Exception {
          return new UserEvent(
              FlatUtil.getPlatformId(event),
              FlatUtil.getUserId(event),
              FlatUtil.getAggMetricValue(event).name());
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
          // TODO: accumulate and provide values dynamically
          return getStandardFeatureIds(CountType.USER_COUNT);
        }

        @Override
        TypeInformation<UserEvent> getTypeInfo() {
          return TypeInformation.of(UserEvent.class);
        }
      };

  UserEventCountKey<LogUserEvent> LOG_USER_EVENT_KEY =
      new UserEventCountKey<>(LogUserEvent.NAME) {
        @Override
        public LogUserEvent getKey(JoinedEvent event) {
          return new LogUserEvent(
              FlatUtil.getPlatformId(event),
              FlatUtil.getLogUserId(event),
              FlatUtil.getAggMetricValue(event).name());
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
          // TODO: accumulate and provide values dynamically
          return getStandardFeatureIds(CountType.LOG_USER_COUNT);
        }

        @Override
        TypeInformation<LogUserEvent> getTypeInfo() {
          return TypeInformation.of(LogUserEvent.class);
        }
      };

  @VisibleForTesting
  LastUserContentKey<LastUserContent> LAST_USER_CONTENT_KEY =
      new LastUserContentKey<>(LastUserContent.NAME) {

        @Override
        public LastUserContent getKey(JoinedEvent joinedEvent) {
          return new LastUserContent(
              FlatUtil.getPlatformId(joinedEvent),
              FlatUtil.getUserId(joinedEvent),
              FlatUtil.getContentId(joinedEvent),
              FlatUtil.getAggMetricValue(joinedEvent).name());
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
          // TODO: accumulate and provide values dynamically
          return getLastUserEventFeatureIds(
              CountType.USER_ITEM_COUNT, CountType.USER_ITEM_HOURS_AGO);
        }

        @Override
        TypeInformation<LastUserContent> getTypeInfo() {
          return TypeInformation.of(LastUserContent.class);
        }
      };

  LastUserContentKey<LastLogUserContent> LAST_LOG_USER_CONTENT_KEY =
      new LastUserContentKey<>(LastLogUserContent.NAME) {
        @Override
        public LastLogUserContent getKey(JoinedEvent joinedEvent) {
          return new LastLogUserContent(
              FlatUtil.getPlatformId(joinedEvent),
              FlatUtil.getLogUserId(joinedEvent),
              FlatUtil.getContentId(joinedEvent),
              FlatUtil.getAggMetricValue(joinedEvent).name());
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
          return getLastUserEventFeatureIds(
              CountType.LOG_USER_ITEM_COUNT, CountType.LOG_USER_ITEM_HOURS_AGO);
        }

        @Override
        TypeInformation<LastLogUserContent> getTypeInfo() {
          return TypeInformation.of(LastLogUserContent.class);
        }
      };

  @VisibleForTesting
  LastUserQueryKey<LastUserQuery> LAST_USER_QUERY_KEY =
      new LastUserQueryKey<>(LastUserQuery.NAME) {

        @Override
        long getQueryHash(LastUserQuery lastUserQuery) {
          return lastUserQuery.getQueryHash();
        }

        @Override
        public LastUserQuery getKey(JoinedEvent joinedEvent) {
          return new LastUserQuery(
              FlatUtil.getPlatformId(joinedEvent),
              FlatUtil.getUserId(joinedEvent),
              FlatUtil.getQueryHash(joinedEvent),
              FlatUtil.getAggMetricValue(joinedEvent).name());
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
          return getLastUserEventFeatureIds(
              CountType.USER_QUERY_COUNT, CountType.USER_QUERY_HOURS_AGO);
        }

        @Override
        TypeInformation<LastUserQuery> getTypeInfo() {
          return TypeInformation.of(LastUserQuery.class);
        }
      };

  LastUserQueryKey<LastLogUserQuery> LAST_LOG_USER_QUERY_KEY =
      new LastUserQueryKey<>(LastLogUserQuery.NAME) {

        @Override
        long getQueryHash(LastLogUserQuery lastLogUserQuery) {
          return lastLogUserQuery.getQueryHash();
        }

        @Override
        public LastLogUserQuery getKey(JoinedEvent joinedEvent) throws Exception {
          return new LastLogUserQuery(
              FlatUtil.getPlatformId(joinedEvent),
              FlatUtil.getUserId(joinedEvent),
              FlatUtil.getQueryHash(joinedEvent),
              FlatUtil.getAggMetricValue(joinedEvent).name());
        }

        @Override
        public ImmutableSet<Long> getFeatureIds() {
          return getLastUserEventFeatureIds(
              CountType.LOG_USER_QUERY_COUNT, CountType.LOG_USER_QUERY_HOURS_AGO);
        }

        @Override
        TypeInformation<LastLogUserQuery> getTypeInfo() {
          return TypeInformation.of(LastLogUserQuery.class);
        }
      };

  private static ImmutableSet<Long> getStandardFeatureIds(CountType type) {
    return FeatureId.expandFeatureIds(
        EnumSet.of(type),
        EnumSet.complementOf(EnumSet.of(AggMetric.UNKNOWN_AGGREGATE, AggMetric.UNRECOGNIZED)),
        EnumSet.of(CountWindow.HOUR, CountWindow.DAY, CountWindow.DAY_7, CountWindow.DAY_30));
  }

  private static ImmutableSet<Long> getLastUserEventFeatureIds(
      CountType count, CountType hoursAgo) {
    return FeatureId.expandFeatureIds(
        ImmutableList.of(
            FeatureId.featureId(count, null, CountWindow.DAY_90),
            FeatureId.featureId(hoursAgo, null, CountWindow.NONE)),
        EnumSet.complementOf(EnumSet.of(AggMetric.UNKNOWN_AGGREGATE, AggMetric.UNRECOGNIZED)));
  }

  abstract class CountKey<KEY> implements KeySelector<JoinedEvent, KEY> {
    private final String name;

    public CountKey(String name) {
      this.name = name;
    }
    /**
     * The name of what is being counted. It's used for logging output paths and operator uid/names.
     */
    String getName() {
      return name;
    }

    /** The string encoding the output row key and value. */
    abstract String getRowFormat();

    /** The set of feature ids provided by this count key. */
    abstract ImmutableSet<Long> getFeatureIds();

    /** The key's type information */
    abstract TypeInformation<KEY> getTypeInfo();
  }

  /**
   * JoinedEventCountKey defines both the KeySelector to extract the list of dimensions to count and
   * a map function to translate from the Sliding*Counter output to a redis command to sink.
   */
  abstract class JoinedEventCountKey<KEY> extends CountKey<KEY>
      implements MapFunction<WindowAggResult<KEY>, RedisSink.Command>,
          ResultTypeQueryable<RedisSink.Command> {

    public JoinedEventCountKey(String name) {
      super(name);
    }

    /** Maps the Sliding*Counter output to the redis command to sink. */
    @Override
    public abstract RedisSink.Command map(WindowAggResult<KEY> in);

    @Override
    public TypeInformation<Command> getProducedType() {
      return TypeInformation.of(RedisSink.Command.class);
    }
  }

  abstract class LastUserEventKey<KEY> extends CountKey<KEY> {
    public LastUserEventKey(String name) {
      super(name);
    }

    abstract RedisSink.Command mapTimestamp(LastTimeAggResult<KEY> in);

    abstract RedisSink.Command mapCount90d(LastTimeAggResult<KEY> in);

    @Override
    public String getRowFormat() {
      return "fid:value";
    }
  }

  @VisibleForTesting
  abstract class UserEventCountKey<KEY extends JoinedEventRedisHashSupplier>
      extends JoinedEventCountKey<KEY> {
    public UserEventCountKey(String name) {
      super(name);
    }

    /**
     * @return platform_id, type, user -> {fid -> count}
     */
    @Override
    public RedisSink.Command map(WindowAggResult<KEY> in) {
      return RedisSink.hsetOrDel(
          in.getKey().getHashKey(),
          in.getKey().getHashField(in.getWindowSize(), in.getWindowUnit()),
          in.getCount(),
          in.getTtl());
    }

    @Override
    public String getRowFormat() {
      return "fid:value";
    }
  }

  abstract class QueryEventCountKey<KEY> extends JoinedEventCountKey<KEY> {

    public QueryEventCountKey(String name) {
      super(name);
    }

    abstract long getQueryHash(KEY key);
  }

  @VisibleForTesting
  abstract class LastUserContentKey<KEY extends LastUserEventRedisHashSupplier>
      extends LastUserEventKey<KEY> {
    LastUserContentKey(String name) {
      super(name);
    }
    /**
     * @return platform_id, type, user, content_id -> {fid -> timestamp}
     */
    @Override
    public RedisSink.Command mapTimestamp(LastTimeAggResult<KEY> in) {
      Tuple key = in.getKey().getHashKey();
      Tuple field = in.getKey().getTimestampHashField();
      if (in.getCount() != 0L) {
        return RedisSink.hset(key, field, in.getTimestamp(), in.getTtl());
      } else {
        return RedisSink.hdel(key, field);
      }
    }

    /**
     * @return platform_id, type, user, content_id -> {fid -> 90d_count}
     */
    @Override
    public RedisSink.Command mapCount90d(LastTimeAggResult<KEY> in) {
      return RedisSink.hsetOrDel(
          in.getKey().getHashKey(),
          in.getKey().getCount90DayHashField(),
          in.getCount(),
          in.getTtl());
    }
  }

  @VisibleForTesting
  abstract class LastUserQueryKey<KEY extends LastUserEventRedisHashSupplier>
      extends LastUserEventKey<KEY> {
    LastUserQueryKey(String name) {
      super(name);
    }

    /**
     * @return platform_id, user_type, user, query_type, query_hex -> {fid -> timestamp}
     */
    @Override
    public RedisSink.Command mapTimestamp(LastTimeAggResult<KEY> in) {
      Tuple key = in.getKey().getHashKey();
      Tuple field = in.getKey().getTimestampHashField();
      if (in.getCount() != 0L) {
        return RedisSink.hset(key, field, Tuple1.of(in.getTimestamp()), 0);
      } else {
        return RedisSink.hdel(key, field);
      }
    }

    /**
     * @return platform_id, user_type, user, query_type, query_hex -> {fid -> 90d_count}
     */
    @Override
    public RedisSink.Command mapCount90d(LastTimeAggResult<KEY> in) {
      return RedisSink.hsetOrDel(
          in.getKey().getHashKey(),
          in.getKey().getCount90DayHashField(),
          in.getCount(),
          in.getTtl());
    }

    abstract long getQueryHash(KEY key);
  }
}
