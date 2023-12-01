package ai.promoted.metrics.logprocessor.job.join.impression;

import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.UnnestedTinyJoinedImpression;
import java.util.Collection;
import java.util.Iterator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Materialize and unnest TinyJoinedImpressions to simplify the joins.
 *
 * <ul>
 *   <li>Materialize joined keys. Fill in missing values on TinyInsertion.
 *   <li>Unnest for other_content_ids.
 * </ul>
 */
public class MaterializeAndUnnestJoinedImpression
    extends ProcessFunction<TinyJoinedImpression, UnnestedTinyJoinedImpression> {

  @Override
  public void processElement(
      TinyJoinedImpression joinedImpression,
      ProcessFunction<TinyJoinedImpression, UnnestedTinyJoinedImpression>.Context context,
      Collector<UnnestedTinyJoinedImpression> collector)
      throws Exception {

    UnnestedTinyJoinedImpression.Builder builder = UnnestedTinyJoinedImpression.newBuilder();
    builder.setJoinedImpression(joinedImpression);

    TinyInsertion insertion = joinedImpression.getInsertion();
    TinyImpression impression = joinedImpression.getImpression();
    builder.setPlatformId(
        firstNonZero(
            insertion.getCommon().getPlatformId(), impression.getCommon().getPlatformId()));
    builder.setAnonUserId(
        StringUtil.firstNotEmpty(
            insertion.getCommon().getAnonUserId(), impression.getCommon().getAnonUserId()));
    // TODO - doing other joins if the IDs are different.
    builder.setContentId(
        StringUtil.firstNotEmpty(insertion.getCore().getContentId(), impression.getContentId()));

    // Optimization - Clone is fairly expensive.
    // Unnest by other_content_id.
    Collection<String> otherContentIds =
        joinedImpression.getInsertion().getCore().getOtherContentIdsMap().values();
    if (otherContentIds.isEmpty()) {
      collector.collect(builder.build());
    } else {
      collector.collect(builder.clone().build());
      builder.setIsOtherContentId(true);
      Iterator<String> otherContentIdsIterator = otherContentIds.iterator();
      while (otherContentIdsIterator.hasNext()) {
        String otherContentId = otherContentIdsIterator.next();
        if (otherContentIdsIterator.hasNext()) {
          collector.collect(builder.clone().setContentId(otherContentId).build());
        } else {
          // Avoid extra clone on the last build.
          collector.collect(builder.setContentId(otherContentId).build());
        }
      }
    }
  }

  private static long firstNonZero(long first, long second) {
    return first != 0L ? first : second;
  }
}
