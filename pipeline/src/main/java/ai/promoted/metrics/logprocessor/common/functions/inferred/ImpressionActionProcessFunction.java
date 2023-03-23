package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.metrics.logprocessor.common.util.TinyFlatUtil;
import ai.promoted.proto.event.TinyEvent;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class ImpressionActionProcessFunction extends ContentId<TinyEvent> {
  // Used as a separator when creating the primary key.  See the comment in the constructor for more
  // details.
  // 0x01 = (^A)
  private static final char SEPARATOR = (char) 0x01;

  private final List<Integer> otherDimensionIds;

  public ImpressionActionProcessFunction(Options options, List<String> otherDimensionIds) {
    super(
        "impression x action",
        (TinyEvent event) -> !event.getImpressionId().isEmpty(),
        TinyFlatUtil::getAllContentIds,
        // The join job splits TinyActions by Shopping CartItem.  It keeps the TinyEvent.action_id
        // the same.
        // This results in duplicates.  This operator needs to treat these tiny actions differently.
        (action) -> action.getActionId() + SEPARATOR + action.getContentId(),
        TinyEvent::getImpressionId,
        TinyFlatUtil::getAllContentIds,
        TinyEvent::getLogTimestamp,
        TinyFlatUtil::mergeAction,
        false,
        options);
    this.otherDimensionIds =
        otherDimensionIds.stream().map(StringUtil::hash).collect(ImmutableList.toImmutableList());
  }

  protected ImmutableList<String> getLeftJoinIds(TinyEvent flat) {
    ImmutableList.Builder<String> builder =
        ImmutableList.<String>builderWithExpectedSize(5 + otherDimensionIds.size())
            .add(flat.getImpressionId())
            .add(flat.getInsertionId())
            .add(flat.getRequestId())
            .add(flat.getViewId())
            .add(flat.getContentId());
    // Include other content IDs in join scopes so we can attribute actions for parent content
    // types.
    otherDimensionIds.forEach(
        key -> {
          String value = flat.getOtherContentIdsOrDefault(key, "");
          if (!value.isEmpty()) {
            builder.add(value);
          }
        });
    return builder.build();
  }

  // TODO - might be able to optimize this just for CHECKOUT and ACTION (or when only contentId is
  // specified).
  protected ImmutableList<String> getRightJoinIds(TinyEvent act) {
    // Fine to have uneven joinIds.
    ImmutableList.Builder<String> builder =
        ImmutableList.<String>builderWithExpectedSize(5 + otherDimensionIds.size())
            .add(act.getImpressionId())
            .add(act.getInsertionId())
            // TODO - if we match by request or view, we should also match by contentId too.
            .add(act.getRequestId())
            .add(act.getViewId())
            .add(act.getContentId());

    // Include other content IDs in join scopes so we can attribute actions for child content types.
    otherDimensionIds.forEach(
        key -> {
          String value = act.getOtherContentIdsOrDefault(key, "");
          if (!value.isEmpty()) {
            builder.add(value);
          }
        });
    return builder.build();
  }

  protected TypeInformation<TinyEvent> getRightTypeInfo() {
    return TypeInformation.of(TinyEvent.class);
  }

  protected boolean debugIdsMatch(TinyEvent rhs) {
    return options.debugIds().matches(rhs);
  }

  protected TinyEvent setLogTimestamp(TinyEvent act, long timestamp) {
    TinyEvent.Builder builder = act.toBuilder();
    builder.setLogTimestamp(timestamp);
    return builder.build();
  }
}
