package ai.promoted.metrics.logprocessor.common.fakedatagenerator.content;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Dummy test DB.  Acts closer to DocumentDB (map).  ID sequence is controlled by the ContentDBFactory.
 * Is serializable for the Minicluster tests.
 */
public class ContentDB implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Multimap<String, Content> emptyMultimap = HashMultimap.create();

    private final Map<String, Content> idToContent;
    // Just a single-level index.
    private final Map<String, Multimap<String, Content>> keyToValueToContent;

    ContentDB() {
        idToContent = new HashMap<>();
        keyToValueToContent = new HashMap<>();
    }

    public void addContent(Content content) {
        idToContent.put(content.contentId(), content);
        content.requestFields().entrySet().forEach(entry -> {
            Multimap<String, Content> valueToContent = keyToValueToContent.get(entry.getKey());
            if (valueToContent == null) {
                valueToContent = HashMultimap.create();
                keyToValueToContent.put(entry.getKey(), valueToContent);
            }
            valueToContent.put(entry.getValue(), content);
        });
    }

    /** Returns the content or null. */
    public Content getContent(String contentId) {
        return idToContent.get(contentId);
    }

    /**
     * Returns all Content sorted by contentId.
     * This is sorted to simplify unit tests.  This does not match how Content DB behaves.
     */
    public ImmutableList<Content> listContent() {
        return listContent(ImmutableList.of());
    }

    /**
     * Returns matching Content sorted by contentId.
     * This is sorted to simplify unit tests.  This does not match how Content DB behaves.
     */
    public ImmutableList<Content> listContent(ContentDBFilter filter) {
        return listContent(ImmutableList.of(filter));
    }

    /**
     * Returns matching Content sorted by contentId.  The multiple filters are AND'ed together.
     * This is sorted to simplify unit tests.  This does not match how Content DB behaves.
     */
    public ImmutableList<Content> listContent(List<ContentDBFilter> filters) {
        if (filters.isEmpty()) {
            // Return everything.
            return idToContent.entrySet().stream().map(Map.Entry::getValue).collect(ImmutableList.toImmutableList());
        }
        // Optimization - Use known maps for the first filter.
        Stream<Content> result;
        ContentDBFilter firstFilter = filters.get(0);
        if (firstFilter.contentId().isPresent()) {
            Content content = idToContent.get(firstFilter.contentId().get());
            result = ImmutableList.of(content).stream().filter(c -> c != null);
        } else {
            result = keyToValueToContent.getOrDefault(firstFilter.fieldKey().get(), emptyMultimap)
                    .get(firstFilter.fieldValue().get())
                    .stream();
        }
        result = applyFilter(result, filters.subList(1, filters.size()));
        return result
                .sorted(Comparator.comparing(c -> c.contentId()))
                .collect(ImmutableList.toImmutableList());
    }

    private Stream<Content> applyFilter(Stream<Content> content, List<ContentDBFilter> filters) {
        if (filters.isEmpty()) {
            return content;
        }
        ContentDBFilter firstFilter = filters.get(0);
        return applyFilter(
                content.filter(c -> {
                    if (firstFilter.contentId().isPresent() && !firstFilter.contentId().get().equals(c.contentId())) {
                        return false;
                    }
                    if (firstFilter.fieldKey().isPresent() && !c.requestFields().get(firstFilter.fieldKey().get()).equals(firstFilter.fieldValue().get())) {
                        return false;
                    }
                    return true;
                }),
                filters.subList(1, filters.size()));
    }
}
