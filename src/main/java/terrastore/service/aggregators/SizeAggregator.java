package terrastore.service.aggregators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import terrastore.store.operators.Aggregator;

/**
 * @author Sergio Bossa
 */
public class SizeAggregator implements Aggregator {

    @Override
    public Map<String, Object> apply(List<Map<String, Object>> counts) {
        int counter = 0;
        for (Map<String, Object> count : counts) {
            counter += (Integer) count.get("size");
        }
        Map<String, Object> size = new HashMap<String, Object>();
        size.put("size", counter);
        return size;
    }
}
