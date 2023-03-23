package ai.promoted.metrics.logprocessor.common.job.testing;

import static org.junit.jupiter.api.Assertions.fail;

import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.regex.Pattern;
import org.apache.flink.api.dag.Transformation;
import picocli.CommandLine;

/**
 * Checks for uid issues (duplicates and format). Should be scoped to a single job.
 *
 * <p>General conventions should be:
 *
 * <ol>
 *   <li>no spaces or silly casing; just lowercased and hyphens
 *   <li>verb -&gt; nouns
 *   <li>specific -&gt; general
 *   <li>use the uid as the name of the operator (and generally keep related outputs simiarly named)
 * </ol>
 *
 * <p>For example, "count-hourly-item-event", "join-impression-action", and
 * "timestamp-and-watermark-source-kafka-view" are appropriate.
 */
public class UidChecker {
  HashSet<String> encounteredUids = new HashSet<>();
  IdentityHashMap<Transformation<?>, Boolean> encounteredTransformations = new IdentityHashMap<>();

  public void check(BaseFlinkJob job) throws Exception {
    checkTransformations(job.sinkTransformations);
    checkTransformationUids(job);
  }

  private void check(String uid) {
    checkForDuplicate(uid);
    checkForUidFormat(uid);
  }

  private void checkForDuplicate(String uid) {
    if (!encounteredUids.add(uid)) {
      fail("Encountered duplicate uid=" + uid);
    }
  }

  private void checkForUidFormat(String uid) {
    if (uid.contains(" ")) {
      fail("Uid should not contain spaces, uid=" + uid);
    }
    Pattern p = Pattern.compile(".*[A-Z].*");
    if (p.matcher(uid).matches()) {
      fail("Uid should not contain capital letters, uid=" + uid);
    }
  }

  // TODO: private once we complete the refactor
  void check(Collection<String> uids) {
    uids.stream().forEach(this::check);
  }

  private void checkTransformationUids(FlinkSegment segment) throws Exception {
    for (Field field : segment.getClass().getDeclaredFields()) {
      if (field.isAnnotationPresent(CommandLine.Mixin.class)
          && FlinkSegment.class.isAssignableFrom(field.getType())) {
        checkTransformationUids((FlinkSegment) field.get(segment));
      } else if ("sinkTransformationUids".equals(field.getName())) {
        @SuppressWarnings("unchecked")
        Collection<String> uids = (Collection<String>) field.get(segment);
        check(uids);
      }
    }
  }

  // TODO: private once we complete the refactor
  void checkTransformations(Collection<Transformation<?>> transformations) {
    transformations.stream().forEach(this::checkTransformation);
  }

  private void checkTransformation(Transformation<?> transformation) {
    if (encounteredTransformations.containsKey(transformation)) {
      // Already visited.
      return;
    }
    encounteredTransformations.put(transformation, true);

    String uid = transformation.getUid();
    if (uid != null) {
      check(uid);
    }
    checkTransformations(transformation.getInputs());
  }
}
