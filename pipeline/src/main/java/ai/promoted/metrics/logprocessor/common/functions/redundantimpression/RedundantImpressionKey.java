package ai.promoted.metrics.logprocessor.common.functions.redundantimpression;

import ai.promoted.proto.event.TinyEvent;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * The key to use for removing redundant impressions.
 *
 * <p>The primary goal for the key design is to remove redundant impressions.
 * <a href="https://docs.google.com/document/d/1sN1l0hYlyDznsBq9_n2c-s-_wwT9cxD5H1E71ycYQck/edit#">Design doc</a>.
 * This can happen for a few reasons:
 * <ul>
 * <li>Auto-refreshing pages.
 * <li>Loading a previous page (reloading or going back).
 * <li>Reloading pages using cached Delivery responses.
 * <li>Bugs in clients and SDKs.
 * <li>RPC retries.  They should have the same record IDs though  There are currently unrelated bugs around this in Flink.
 * <li>Bots - Long-term, we’ll try filtering these out.
 * </ul>
 *
 * <p>A secondary goal for the key design is to have better support for MRC impressions.
 * <a href="http://www.mediaratingcouncil.org/063014%20Viewable%20Ad%20Impression%20Guideline_Final.pdf">Spec</a>.
 *
 * <p>There is not a perfect key design unless we want to have complex redundancy logic and introduce time delays.
 * This key keeps the logic pretty simple.  There are corner cases when content can show up as insertions and
 * non-insertions.
 *
 * <p>TODO - change the key to a more specific proto message so it's more flexible.
 */
public final class RedundantImpressionKey {

    // If insertionId is specified,
    // Then (platformId, logUserId, insertionId, empty string).
    // Else (platformId, logUserId, autoViewId or viewId, contentId).
    public static Tuple4<Long, String, String, String> of(TinyEvent event) {
        if (!event.getInsertionId().isEmpty()) {
            return Tuple4.of(
                    event.getPlatformId(),
                    event.getLogUserId(),
                    event.getInsertionId(),
                    "");
        } else {
            return Tuple4.of(
                    event.getPlatformId(),
                    event.getLogUserId(),
                    event.getViewId(),
                    event.getContentId());
        }
    }

    private RedundantImpressionKey() {
    }
}