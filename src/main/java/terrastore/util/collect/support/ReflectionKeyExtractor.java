package terrastore.util.collect.support;

import java.lang.reflect.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Sergio Bossa
 */
public class ReflectionKeyExtractor<K, V> implements KeyExtractor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(ReflectionKeyExtractor.class);
    private final String fieldName;

    public ReflectionKeyExtractor(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public K extractFrom(V value) {
        try {
            Field field = value.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (K) field.get(value);
        } catch (Exception ex) {
            LOG.warn(ex.getMessage(), ex);
            throw new IllegalStateException(ex);
        }
    }
}
