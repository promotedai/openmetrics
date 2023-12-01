package ai.promoted.metrics.logprocessor.common.functions.inferred;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;

/** A tuple used to a primaryKey and foreignKeys. Useful when tracking LHS keys. */
@AutoValue
abstract class JoinKeys implements Serializable {
  abstract String primaryKey();

  abstract List<String> foreignKeys();

  public static JoinKeys create(String primaryKey, List<String> foreignKeys) {
    return new AutoValue_JoinKeys(primaryKey, foreignKeys);
  }
}
