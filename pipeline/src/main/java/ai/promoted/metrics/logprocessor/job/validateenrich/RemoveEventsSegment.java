package ai.promoted.metrics.logprocessor.job.validateenrich;

import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.proto.event.Action;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.functions.FilterFunction;
import picocli.CommandLine.Option;

/** A {@code FlinkSegment} that allows us to remove specific ranges of events. */
public class RemoveEventsSegment implements FlinkSegment {
  @Option(
      names = {"--customActionTypeToRemoveEventApiTimestampRange"},
      description =
          "A Map of Action.customActionTypes to a TimeRange to remove.  EventApiTimestamp is used since clients might backfill the same clientLogTimestamp range.")
  public Map<String, String> customActionTypeToRemoveEventApiTimestampRange = new HashMap<>();

  // For now, just support one removal time range.  It keeps the code smaller for a hacky fix.
  // If a customer needs multiple, implement it then.
  private Map<String, TimeRange> parsedCustomActionTypeToRemoveEventApiTimestampRange;

  @Override
  public void validateArgs() {
    // Calling this validates the inputs.
    getParsedCustomActionTypeToRemoveEventApiTimestampRange();
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.of();
  }

  public boolean canRemoveActions() {
    return !customActionTypeToRemoveEventApiTimestampRange.isEmpty();
  }

  public FilterFunction<Action> getKeepActionsPredicate() {
    final Map<String, TimeRange> removeTimeRangesMap =
        getParsedCustomActionTypeToRemoveEventApiTimestampRange();
    return new FilterFunction<Action>() {
      @Override
      public boolean filter(Action action) throws Exception {
        TimeRange excludedTimeRange = removeTimeRangesMap.get(action.getCustomActionType());
        if (excludedTimeRange != null) {
          long eventApiTimestamp = action.getTiming().getEventApiTimestamp();
          return eventApiTimestamp >= excludedTimeRange.endTimeExclusive()
              || eventApiTimestamp < excludedTimeRange.startTimeInclusive();
        }
        return true;
      }
    };
  }

  private Map<String, TimeRange> getParsedCustomActionTypeToRemoveEventApiTimestampRange() {
    if (parsedCustomActionTypeToRemoveEventApiTimestampRange == null) {
      parsedCustomActionTypeToRemoveEventApiTimestampRange = new HashMap<>();
      for (Map.Entry<String, String> entry :
          customActionTypeToRemoveEventApiTimestampRange.entrySet()) {
        parsedCustomActionTypeToRemoveEventApiTimestampRange.put(
            entry.getKey(), parseTimeRange(entry.getValue()));
      }
    }
    return parsedCustomActionTypeToRemoveEventApiTimestampRange;
  }

  @VisibleForTesting
  static TimeRange parseTimeRange(String timeRange) {
    List<String> parts = Splitter.on(",").splitToList(timeRange);
    Preconditions.checkArgument(parts.size() == 2, "TimeRange needs exactly one comma");
    long startTimeInclusive = parseLong(parts.get(0), "startTime");
    long endTimeExclusive = parseLong(parts.get(1), "endTime");
    return TimeRange.create(startTimeInclusive, endTimeExclusive);
  }

  static long parseLong(String value, String name) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " needs to be a long epoch millis; value=" + value);
    }
  }
}
