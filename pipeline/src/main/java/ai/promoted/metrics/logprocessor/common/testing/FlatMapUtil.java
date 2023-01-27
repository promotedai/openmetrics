package ai.promoted.metrics.logprocessor.common.testing;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A utiltiy to flatten nested Maps.  This simplifies reading assertions.
 */
public class FlatMapUtil {

    public static Map<String, Object> flatten(Map<String, Object> map) {
        return flattenMap("", map).collect(
                HashMap::new,
                (collector, entry) -> collector.put(entry.getKey(), entry.getValue()),
                HashMap::putAll);
    }

    private static Stream<Map.Entry<String, Object>> flattenEntry(Map.Entry<String, Object> entry) {
        Object value = entry.getValue();
        if (value instanceof Map) {
            return flattenMap(entry.getKey(), (Map) value);
        }
        if (value instanceof List) {
            Stream<Map.Entry<String, Object>> entries = flattenList((List) value);
            return entries.map(valueEntry -> new AbstractMap.SimpleEntry<>(
                    entry.getKey() + "/" + valueEntry.getKey(),
                    valueEntry.getValue()));
        }
        return Stream.of(entry);
    }

    private static Stream<Map.Entry<String, Object>> flattenMap(String parentKey, Map<String, Object> map) {
        return map.entrySet()
                .stream()
                .flatMap(entry -> flattenEntry(
                        new AbstractMap.SimpleEntry<>(parentKey + "/" + entry.getKey(), entry.getValue())));
    }

    private static Stream<Map.Entry<String, Object>> flattenList(List<?> list) {
        return IntStream.range(0, list.size())
                .mapToObj(i -> new AbstractMap.SimpleEntry<String, Object>(Integer.toString(i), list.get(i)))
                .flatMap(FlatMapUtil::flattenEntry);
    }
}
