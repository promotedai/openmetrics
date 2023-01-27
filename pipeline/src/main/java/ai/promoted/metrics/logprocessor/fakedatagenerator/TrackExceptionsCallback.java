package ai.promoted.metrics.logprocessor.fakedatagenerator;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;

class TrackExceptionsCallback implements Callback {
    private final List<Exception> exceptions = new ArrayList<>();

    @Override
    public void onCompletion(RecordMetadata ignored, Exception e) {
        if (e != null) {
            exceptions.add(e);
        }
    }

    public void throwIfHasException() throws MultipleKafkaExceptions {
        if (!exceptions.isEmpty()) {
            throw new MultipleKafkaExceptions(exceptions);
        }
    }

    public static final class MultipleKafkaExceptions extends Exception {
        private final ImmutableList<Exception> exceptions;

        public MultipleKafkaExceptions(List<Exception> exceptions) {
            super(exceptions.size() + " Exceptions were encountered when writing to Kafka", exceptions.get(0));
            this.exceptions = ImmutableList.copyOf(exceptions);
        }

        public void printStackTrace() {
            for (Exception exception : exceptions) {
                exception.printStackTrace();
            }
        }
    }
}
