package ai.promoted.metrics.logprocessor.common.testing;

import com.google.common.collect.ImmutableList;
import java.util.Iterator;

/** Iterable utility. */
public interface Iterables {

  /**
   * @return the element if there is only one, otherwise throws IllegalArgumentException
   */
  static <T> T exactlyOne(Iterable<T> iterable) {
    Iterator<T> iterator = iterable.iterator();
    if (!iterator.hasNext()) {
      throw new IllegalArgumentException("Iterable should have an item but does not");
    }
    T item = iterator.next();
    if (iterator.hasNext()) {
      throw new IllegalArgumentException(
          "Iterable should only have one item, iterable=" + ImmutableList.copyOf(iterable));
    }
    return item;
  }
}
