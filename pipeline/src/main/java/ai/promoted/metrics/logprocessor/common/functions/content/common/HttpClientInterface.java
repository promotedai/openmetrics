package ai.promoted.metrics.logprocessor.common.functions.content.common;

import java.io.IOException;
import java.io.Serializable;
import java.net.http.HttpResponse;
import java.util.Collection;

/**
 * Interface to mock out the HttpClient in unit tests.
 */
interface HttpClientInterface extends Serializable {

    HttpResponse send(Collection<String> contentIds) throws IOException, InterruptedException;
}
