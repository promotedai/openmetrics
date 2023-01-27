package ai.promoted.metrics.logprocessor.common.functions;

import com.google.common.base.Preconditions;

import javax.annotation.CheckForNull;
import java.util.function.Predicate;

public class SerializablePredicates {

    public static <T> SerializablePredicate<T> not(SerializablePredicate<T> predicate) {
        return new NotPredicate(predicate);
    }

    // Partially copied from com.google.common.base.Predicates.
    private static class NotPredicate<T> implements SerializablePredicate<T> {
        final Predicate<T> predicate;
        private static final long serialVersionUID = 0L;

        NotPredicate(SerializablePredicate<T> predicate) {
            this.predicate = Preconditions.checkNotNull(predicate);
        }

        @Override
        public boolean test(T t) {
            return !this.predicate.test(t);
        }

        public int hashCode() {
            return ~this.predicate.hashCode();
        }

        public boolean equals(@CheckForNull Object obj) {
            if (obj instanceof NotPredicate) {
                NotPredicate<?> that = (NotPredicate) obj;
                return this.predicate.equals(that.predicate);
            } else {
                return false;
            }
        }

        public String toString() {
            String predicateString = String.valueOf(this.predicate);
            return (new StringBuilder(28 + String.valueOf(predicateString).length())).append("SerializablePredicates.not(").append(predicateString).append(")").toString();
        }
    }
}