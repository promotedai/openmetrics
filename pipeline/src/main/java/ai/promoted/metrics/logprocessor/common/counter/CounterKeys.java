package ai.promoted.metrics.logprocessor.common.counter;

import static ai.promoted.metrics.logprocessor.common.counter.Constants.TYPE_SEPARATOR;

import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink.RedisSinkCommand;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.delivery.internal.features.AggMetric;
import ai.promoted.proto.delivery.internal.features.CountType;
import ai.promoted.proto.delivery.internal.features.CountWindow;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.JoinedImpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import ua_parser.Parser;

// TODO - filter out AttributedActions.

/** "Interface" used for namespacing keys related to counters. */
public final class CounterKeys {
  public static final String ROW_FORMAT_KEY = TYPE_SEPARATOR + RedisSink.JOIN_CHAR + "row_format";
  public static final String FEATURE_IDS_KEY = TYPE_SEPARATOR + RedisSink.JOIN_CHAR + "feature_ids";
  public static final Parser UAParser = new Parser();
  private final QueryEventKey queryEventKey;
  private final ContentQueryEventKey contentQueryEventKey;
  private final GlobalEventDeviceKey globalEventDeviceKey;
  private final ContentEventDeviceKey contentEventDeviceKey;
  private final UserEventKey userEventKey;
  private final LogUserEventKey logUserEventKey;
  private final LastUserContentKey lastUserContentKey;
  private final LastLogUserContentKey lastLogUserContentKey;
  private final LastUserQueryKey lastUserQueryKey;
  private final LastLogUserQueryKey lastLogUserQueryKey;

  public CounterKeys(boolean enable90d) {
    this.queryEventKey = new QueryEventKey(enable90d);
    this.contentQueryEventKey = new ContentQueryEventKey(enable90d);
    this.globalEventDeviceKey = new GlobalEventDeviceKey(enable90d);
    this.contentEventDeviceKey = new ContentEventDeviceKey(enable90d);
    this.userEventKey = new UserEventKey(enable90d);
    this.logUserEventKey = new LogUserEventKey(enable90d);
    this.lastUserContentKey = new LastUserContentKey();
    this.lastLogUserContentKey = new LastLogUserContentKey();
    this.lastUserQueryKey = new LastUserQueryKey();
    this.lastLogUserQueryKey = new LastLogUserQueryKey();
  }

  private static ImmutableSet<Long> getStandardFeatureIds(CountType type, boolean enable90d) {
    EnumSet<CountWindow> countWindow =
        EnumSet.of(CountWindow.HOUR, CountWindow.DAY, CountWindow.DAY_7, CountWindow.DAY_30);
    if (enable90d) {
      countWindow.add(CountWindow.DAY_90);
    }
    return FeatureId.expandFeatureIds(
        EnumSet.of(type),
        EnumSet.complementOf(EnumSet.of(AggMetric.UNKNOWN_AGGREGATE, AggMetric.UNRECOGNIZED)),
        countWindow);
  }

  private static ImmutableSet<Long> getLastUserEventFeatureIds(
      CountType count, CountType hoursAgo) {
    return FeatureId.expandFeatureIds(
        ImmutableList.of(
            FeatureId.featureId(count, null, CountWindow.DAY_90),
            FeatureId.featureId(hoursAgo, null, CountWindow.NONE)),
        EnumSet.complementOf(EnumSet.of(AggMetric.UNKNOWN_AGGREGATE, AggMetric.UNRECOGNIZED)));
  }

  private static void addCountKey(
      ImmutableMap.Builder<String, CountKey<?>> builder, CountKey<?> key) {
    builder.put(key.getName(), key);
  }

  public QueryEventKey getQueryEventKey() {
    return queryEventKey;
  }

  public ContentQueryEventKey getContentQueryEventKey() {
    return contentQueryEventKey;
  }

  public GlobalEventDeviceKey getGlobalEventDeviceKey() {
    return globalEventDeviceKey;
  }

  public ContentEventDeviceKey getContentEventDeviceKey() {
    return contentEventDeviceKey;
  }

  public UserEventKey getUserEventKey() {
    return userEventKey;
  }

  public LogUserEventKey getLogUserEventKey() {
    return logUserEventKey;
  }

  public LastUserContentKey getLastUserContentKey() {
    return lastUserContentKey;
  }

  public LastLogUserContentKey getLastLogUserContentKey() {
    return lastLogUserContentKey;
  }

  public LastUserQueryKey getLastUserQueryKey() {
    return lastUserQueryKey;
  }

  public LastLogUserQueryKey getLastLogUserQueryKey() {
    return lastLogUserQueryKey;
  }

  public Map<String, CountKey<?>> getAllKeys() {
    ImmutableMap.Builder<String, CountKey<?>> builder = ImmutableMap.builder();
    addCountKey(builder, getQueryEventKey());
    addCountKey(builder, getContentQueryEventKey());
    addCountKey(builder, getGlobalEventDeviceKey());
    addCountKey(builder, getContentEventDeviceKey());
    addCountKey(builder, getUserEventKey());
    addCountKey(builder, getLogUserEventKey());
    addCountKey(builder, getLastUserContentKey());
    addCountKey(builder, getLastLogUserContentKey());
    addCountKey(builder, getLastUserQueryKey());
    addCountKey(builder, getLastLogUserQueryKey());
    return builder.build();
  }

  public static class LastLogUserQueryKey extends BaseLastUserQueryKey<PlatformLogUserQueryEvent> {
    public LastLogUserQueryKey() {
      super(PlatformLogUserQueryEvent.NAME, PlatformLogUserQueryEvent.DEPRECATED_NAME);
    }

    @Override
    public PlatformLogUserQueryEvent getKey(JoinedImpression impression, String aggMetric) {
      return new PlatformLogUserQueryEvent(
          impression.getIds().getPlatformId(),
          impression.getIds().getLogUserId(),
          FlatUtil.getQueryHash(impression.getRequest()),
          aggMetric);
    }

    @Override
    public long getQueryHash(PlatformLogUserQueryEvent platformLogUserQueryEvent) {
      return platformLogUserQueryEvent.getQueryHash();
    }

    @Override
    public ImmutableSet<Long> getFeatureIds() {
      return getLastUserEventFeatureIds(
          CountType.LOG_USER_QUERY_COUNT, CountType.LOG_USER_QUERY_HOURS_AGO);
    }

    @Override
    public TypeInformation<PlatformLogUserQueryEvent> getTypeInfo() {
      return TypeInformation.of(PlatformLogUserQueryEvent.class);
    }
  }

  public static class LastUserQueryKey extends BaseLastUserQueryKey<PlatformUserQueryEvent> {
    public LastUserQueryKey() {
      super(PlatformUserQueryEvent.NAME, PlatformUserQueryEvent.DEPRECATED_NAME);
    }

    @Override
    public PlatformUserQueryEvent getKey(JoinedImpression impression, String aggMetric) {
      return new PlatformUserQueryEvent(
          impression.getIds().getPlatformId(),
          impression.getIds().getUserId(),
          FlatUtil.getQueryHash(impression.getRequest()),
          aggMetric);
    }

    @Override
    public long getQueryHash(PlatformUserQueryEvent platformUserQueryEvent) {
      return platformUserQueryEvent.getQueryHash();
    }

    @Override
    public ImmutableSet<Long> getFeatureIds() {
      return getLastUserEventFeatureIds(CountType.USER_QUERY_COUNT, CountType.USER_QUERY_HOURS_AGO);
    }

    @Override
    public TypeInformation<PlatformUserQueryEvent> getTypeInfo() {
      return TypeInformation.of(PlatformUserQueryEvent.class);
    }
  }

  public static class LastLogUserContentKey
      extends BaseLastUserContentKey<PlatformLogUserContentEvent> {
    // TODO - why was enable90d in here before?

    public LastLogUserContentKey() {
      super(PlatformLogUserContentEvent.NAME, PlatformLogUserContentEvent.DEPRECATED_NAME);
    }

    @Override
    public PlatformLogUserContentEvent getKey(JoinedImpression impression, String aggMetric) {
      return new PlatformLogUserContentEvent(
          impression.getIds().getPlatformId(),
          impression.getIds().getLogUserId(),
          impression.getResponseInsertion().getContentId(),
          aggMetric);
    }

    @Override
    public ImmutableSet<Long> getFeatureIds() {
      return getLastUserEventFeatureIds(
          CountType.LOG_USER_ITEM_COUNT, CountType.LOG_USER_ITEM_HOURS_AGO);
    }

    @Override
    public TypeInformation<PlatformLogUserContentEvent> getTypeInfo() {
      return TypeInformation.of(PlatformLogUserContentEvent.class);
    }
  }

  public static class LastUserContentKey extends BaseLastUserContentKey<PlatformUserContentEvent> {
    public LastUserContentKey() {
      super(PlatformUserContentEvent.NAME, PlatformUserContentEvent.DEPRECATED_NAME);
    }

    @Override
    public PlatformUserContentEvent getKey(JoinedImpression impression, String aggMetric) {
      return new PlatformUserContentEvent(
          impression.getIds().getPlatformId(),
          impression.getIds().getUserId(),
          impression.getResponseInsertion().getContentId(),
          aggMetric);
    }

    @Override
    public ImmutableSet<Long> getFeatureIds() {
      // TODO: accumulate and provide values dynamically
      return getLastUserEventFeatureIds(CountType.USER_ITEM_COUNT, CountType.USER_ITEM_HOURS_AGO);
    }

    @Override
    public TypeInformation<PlatformUserContentEvent> getTypeInfo() {
      return TypeInformation.of(PlatformUserContentEvent.class);
    }
  }

  public static class LogUserEventKey extends UserEventCountKey<PlatformLogUserEvent> {
    private final boolean enable90d;

    public LogUserEventKey(boolean enable90d) {
      super(PlatformLogUserEvent.NAME, PlatformLogUserEvent.DEPRECATED_NAME);
      this.enable90d = enable90d;
    }

    @Override
    public PlatformLogUserEvent getKey(JoinedImpression impression, String aggMetric) {
      return new PlatformLogUserEvent(
          impression.getIds().getPlatformId(), impression.getIds().getLogUserId(), aggMetric);
    }

    @Override
    public ImmutableSet<Long> getFeatureIds() {
      // TODO: accumulate and provide values dynamically
      return getStandardFeatureIds(CountType.LOG_USER_COUNT, enable90d);
    }

    @Override
    public TypeInformation<PlatformLogUserEvent> getTypeInfo() {
      return TypeInformation.of(PlatformLogUserEvent.class);
    }
  }

  public static class UserEventKey extends UserEventCountKey<PlatformUserEvent> {
    private final boolean enable90d;

    public UserEventKey(boolean enable90d) {
      super(PlatformUserEvent.NAME, PlatformUserEvent.DEPRECATED_NAME);
      this.enable90d = enable90d;
    }

    @Override
    public PlatformUserEvent getKey(JoinedImpression impression, String aggMetric) {
      return new PlatformUserEvent(
          impression.getIds().getPlatformId(), impression.getIds().getUserId(), aggMetric);
    }

    @Override
    public ImmutableSet<Long> getFeatureIds() {
      // TODO: accumulate and provide values dynamically
      return getStandardFeatureIds(CountType.USER_COUNT, enable90d);
    }

    @Override
    public TypeInformation<PlatformUserEvent> getTypeInfo() {
      return TypeInformation.of(PlatformUserEvent.class);
    }
  }

  public static class ContentEventDeviceKey
      extends JoinedEventCountKey<PlatformContentDeviceEvent> {
    private final boolean enable90d;

    public ContentEventDeviceKey(boolean enable90d) {
      super(PlatformContentDeviceEvent.NAME, PlatformContentDeviceEvent.DEPRECATED_NAME);
      this.enable90d = enable90d;
    }

    @Override
    public TypeInformation<PlatformContentDeviceEvent> getTypeInfo() {
      return TypeInformation.of(PlatformContentDeviceEvent.class);
    }

    @Override
    public PlatformContentDeviceEvent getKey(JoinedImpression impression, String aggMetric) {
      String requestAgent = impression.getRequest().getDevice().getBrowser().getUserAgent();
      String device =
          Device.getDevice(
              UAParser.parseOS(requestAgent).family, UAParser.parseUserAgent(requestAgent).family);
      return new PlatformContentDeviceEvent(
          impression.getIds().getPlatformId(),
          impression.getResponseInsertion().getContentId(),
          device,
          aggMetric);
    }

    @Override
    public RedisSinkCommand map(WindowAggResult<PlatformContentDeviceEvent> in) {
      return RedisSink.hsetOrDel(
          in.getKey().toRedisHashKey(),
          in.getKey().toRedisHashField(in.getWindowSize(), in.getWindowUnit()),
          in.getCount(),
          in.getTtl());
    }

    @Override
    public String getRowFormat() {
      return "device,fid:value";
    }

    @Override
    public ImmutableSet<Long> getFeatureIds() {
      return getStandardFeatureIds(CountType.ITEM_DEVICE_COUNT, enable90d);
    }
  }

  public static class GlobalEventDeviceKey extends JoinedEventCountKey<PlatformDeviceEvent> {

    private final boolean enable90d;

    public GlobalEventDeviceKey(boolean enable90d) {
      super(PlatformDeviceEvent.NAME, PlatformDeviceEvent.DEPRECATED_NAME);
      this.enable90d = enable90d;
    }

    @Override
    public TypeInformation<PlatformDeviceEvent> getTypeInfo() {
      return TypeInformation.of(PlatformDeviceEvent.class);
    }

    @Override
    public PlatformDeviceEvent getKey(JoinedImpression impression, String aggMetric) {
      String requestAgent = impression.getRequest().getDevice().getBrowser().getUserAgent();
      String device =
          Device.getDevice(
              UAParser.parseOS(requestAgent).family, UAParser.parseUserAgent(requestAgent).family);
      return new PlatformDeviceEvent(impression.getIds().getPlatformId(), device, aggMetric);
    }

    @Override
    public RedisSinkCommand map(WindowAggResult<PlatformDeviceEvent> in) {
      return RedisSink.hsetOrDel(
          in.getKey().toRedisHashKey(),
          in.getKey().toRedisHashField(in.getWindowSize(), in.getWindowUnit()),
          in.getCount(),
          -1 /* intentionally don't ever expire globals */);
    }

    @Override
    public String getRowFormat() {
      return "device,fid:value";
    }

    @Override
    public ImmutableSet<Long> getFeatureIds() {
      return getStandardFeatureIds(CountType.ITEM_DEVICE_COUNT, enable90d);
    }
  }

  public static class ContentQueryEventKey extends QueryEventCountKey<PlatformContentQueryEvent> {
    private final boolean enable90d;

    public ContentQueryEventKey(boolean enable90d) {
      super(PlatformContentQueryEvent.NAME, PlatformContentQueryEvent.DEPRECATED_NAME);
      this.enable90d = enable90d;
    }

    @Override
    public TypeInformation<PlatformContentQueryEvent> getTypeInfo() {
      return TypeInformation.of(PlatformContentQueryEvent.class);
    }

    @Override
    public PlatformContentQueryEvent getKey(JoinedImpression impression, String aggMetric) {
      return new PlatformContentQueryEvent(
          impression.getIds().getPlatformId(),
          FlatUtil.getQueryHash(impression.getRequest()),
          impression.getResponseInsertion().getContentId(),
          aggMetric);
    }

    @Override
    public long getQueryHash(PlatformContentQueryEvent key) {
      return key.getQueryHash();
    }

    /**
     * @return platform_id, content_id, type, query_hex -> {fid -> count}
     */
    @Override
    public RedisSinkCommand map(WindowAggResult<PlatformContentQueryEvent> in) {
      return RedisSink.hsetOrDel(
          in.getKey().toRedisHashKey(),
          in.getKey().toRedisHashField(in.getWindowSize(), in.getWindowUnit()),
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
      return getStandardFeatureIds(CountType.ITEM_QUERY_COUNT, enable90d);
    }
  }

  public static class QueryEventKey extends QueryEventCountKey<PlatformQueryEvent> {
    private final boolean enable90d;

    public QueryEventKey(boolean enable90d) {
      super(PlatformQueryEvent.NAME, PlatformQueryEvent.DEPRECATED_NAME);
      this.enable90d = enable90d;
    }

    @Override
    public TypeInformation<PlatformQueryEvent> getTypeInfo() {
      return TypeInformation.of(PlatformQueryEvent.class);
    }

    @Override
    public PlatformQueryEvent getKey(JoinedImpression impression, String aggMetric) {
      return new PlatformQueryEvent(
          impression.getIds().getPlatformId(),
          FlatUtil.getQueryHash(impression.getRequest()),
          aggMetric);
    }

    @Override
    public long getQueryHash(PlatformQueryEvent key) {
      return key.getQueryHash();
    }

    /**
     * @return platform_id, type, query_hex -> {fid -> count}
     */
    @Override
    public RedisSinkCommand map(WindowAggResult<PlatformQueryEvent> in) {
      return RedisSink.hsetOrDel(
          in.getKey().toRedisHashKey(),
          in.getKey().toRedisHashField(in.getWindowSize(), in.getWindowUnit()),
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
      return getStandardFeatureIds(CountType.QUERY_COUNT, enable90d);
    }
  }

  public abstract static class CountKey<KEY> implements Serializable {
    private final String name;
    private final String deprecatedRedisName;

    public CountKey(String name, String deprecatedRedisName) {
      this.name = name;
      this.deprecatedRedisName = deprecatedRedisName;
    }

    /**
     * The name of what is being counted. It's used for logLastLogUserQueryKeging output paths and
     * operator uid/names.
     */
    public String getName() {
      return name;
    }

    public String getDeprecatedRedisName() {
      return deprecatedRedisName;
    }

    public KEY extractKey(JoinedImpression impression) {
      return getKey(impression, AggMetric.COUNT_IMPRESSION.name());
    }

    public KEY extractKey(AttributedAction action) {
      // Counts for Actions use Insertion details so the counts are related to Insertions for
      // predictions.
      return getKey(
          action.getTouchpoint().getJoinedImpression(), FlatUtil.getAggMetricValue(action).name());
    }

    public abstract KEY getKey(JoinedImpression impression, String aggMetric);

    /** The string encoding the output row key and value. */
    public abstract String getRowFormat();

    /** The set of feature ids provided by this count key. */
    public abstract ImmutableSet<Long> getFeatureIds();

    /** The key's type information */
    public abstract TypeInformation<KEY> getTypeInfo();
  }

  /**
   * JoinedEventCountKey defines both the KeySelector to extract the list of dimensions to count and
   * a map function to translate from the Sliding*Counter output to a redis command to sink.
   */
  public abstract static class JoinedEventCountKey<KEY> extends CountKey<KEY>
      implements MapFunction<WindowAggResult<KEY>, RedisSinkCommand>,
          ResultTypeQueryable<RedisSinkCommand> {

    public JoinedEventCountKey(String name, String deprecatedRedisName) {
      super(name, deprecatedRedisName);
    }

    /** Maps the Sliding*Counter output to the redis command to sink. */
    @Override
    public abstract RedisSinkCommand map(WindowAggResult<KEY> in);

    @Override
    public TypeInformation<RedisSinkCommand> getProducedType() {
      return TypeInformation.of(RedisSinkCommand.class);
    }
  }

  public abstract static class LastUserEventKey<KEY> extends CountKey<KEY> {
    public LastUserEventKey(String name, String deprecatedRedisName) {
      super(name, deprecatedRedisName);
    }

    public abstract RedisSinkCommand mapTimestamp(LastTimeAggResult<KEY> in);

    public abstract RedisSinkCommand mapCount90d(LastTimeAggResult<KEY> in);

    @Override
    public String getRowFormat() {
      return "fid:value";
    }
  }

  @VisibleForTesting
  abstract static class UserEventCountKey<KEY extends JoinedEventRedisHashSupplier>
      extends JoinedEventCountKey<KEY> {
    public UserEventCountKey(String name, String deprecatedRedisName) {
      super(name, deprecatedRedisName);
    }

    /**
     * @return platform_id, type, user -> {fid -> count}
     */
    @Override
    public RedisSinkCommand map(WindowAggResult<KEY> in) {
      return RedisSink.hsetOrDel(
          in.getKey().toRedisHashKey(),
          in.getKey().toRedisHashField(in.getWindowSize(), in.getWindowUnit()),
          in.getCount(),
          in.getTtl());
    }

    @Override
    public String getRowFormat() {
      return "fid:value";
    }
  }

  public abstract static class QueryEventCountKey<KEY> extends JoinedEventCountKey<KEY> {

    public QueryEventCountKey(String name, String deprecatedRedisName) {
      super(name, deprecatedRedisName);
    }

    public abstract long getQueryHash(KEY key);
  }

  @VisibleForTesting
  abstract static class BaseLastUserContentKey<KEY extends UserEventRedisHashSupplier>
      extends LastUserEventKey<KEY> {
    BaseLastUserContentKey(String name, String deprecatedRedisName) {
      super(name, deprecatedRedisName);
    }

    /**
     * @return platform_id, type, user, content_id -> {fid -> timestamp}
     */
    @Override
    public RedisSinkCommand mapTimestamp(LastTimeAggResult<KEY> in) {
      Tuple key = in.getKey().toRedisHashKey();
      Tuple field = in.getKey().toLastTimestampRedisHashField();
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
    public RedisSinkCommand mapCount90d(LastTimeAggResult<KEY> in) {
      return RedisSink.hsetOrDel(
          in.getKey().toRedisHashKey(),
          in.getKey().toCount90DayRedisHashField(),
          in.getCount(),
          in.getTtl());
    }
  }

  @VisibleForTesting
  public abstract static class BaseLastUserQueryKey<KEY extends UserEventRedisHashSupplier>
      extends LastUserEventKey<KEY> {
    BaseLastUserQueryKey(String name, String deprecatedRedisName) {
      super(name, deprecatedRedisName);
    }

    /**
     * @return platform_id, user_type, user, query_type, query_hex -> {fid -> timestamp}
     */
    @Override
    public RedisSinkCommand mapTimestamp(LastTimeAggResult<KEY> in) {
      Tuple key = in.getKey().toRedisHashKey();
      Tuple field = in.getKey().toLastTimestampRedisHashField();
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
    public RedisSinkCommand mapCount90d(LastTimeAggResult<KEY> in) {
      return RedisSink.hsetOrDel(
          in.getKey().toRedisHashKey(),
          in.getKey().toCount90DayRedisHashField(),
          in.getCount(),
          in.getTtl());
    }

    public abstract long getQueryHash(KEY key);
  }
}
