package terrastore.util.collect.support;

/**
 * @author Sergio Bossa
 */
public interface KeyExtractor<K, V> {

    public K extractFrom(V value);
}
