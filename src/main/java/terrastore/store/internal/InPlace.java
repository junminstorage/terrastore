package terrastore.store.internal;

import java.util.Map;
import terrastore.store.Value;

/**
 * @author Sergio Bossa
 */
public interface InPlace {

    public Value apply(String key, Value value, Map<String, Object> parameters);
}
