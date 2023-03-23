package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.proto.event.InsertionBundle;
import com.google.auto.value.AutoValue;
import java.io.Serializable;

/** A tuple used to separate state inside of MergeImpressionDetails. */
@AutoValue
abstract class InsertionIdAndBundle implements Serializable {
  abstract String insertionId();

  abstract InsertionBundle bundle();

  public static InsertionIdAndBundle create(String insertionId, InsertionBundle bundle) {
    return new AutoValue_InsertionIdAndBundle(insertionId, bundle);
  }
}
