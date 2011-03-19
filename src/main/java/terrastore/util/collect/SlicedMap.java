package terrastore.util.collect;

import com.google.common.collect.AbstractIterator;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class SlicedMap<K, V> extends AbstractMap<K, V> {

    private final Map<K, V> source;
    private final Set<K> slice;

    public SlicedMap(Map<K, V> source, Set<K> slice) {
        this.source = source;
        this.slice = slice;
    }

    @Override
    public V get(Object key) {
        if (slice.contains(key) && source.containsKey(key)) {
            return source.get(key);
        } else {
            return null;
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new SlicedSet();
    }

    private class SlicedSet extends AbstractSet<Entry<K, V>> {

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new SlicedIterator();
        }

        @Override
        public int size() {
            Iterator iterator = new SlicedIterator();
            int size = 0;
            while (iterator.hasNext()) {
                iterator.next();
                size++;
            }
            return size;
        }

        private class SlicedIterator extends AbstractIterator<Entry<K, V>> {

            private final Iterator<K> slicedIterator;

            public SlicedIterator() {
                slicedIterator = slice.iterator();
            }

            @Override
            protected Entry<K, V> computeNext() {
                while (slicedIterator.hasNext()) {
                    K nextKey = slicedIterator.next();
                    V nextValue = source.get(nextKey);
                    if (nextValue != null) {
                        return new SlicedEntry(nextKey, nextValue);
                    }
                }
                return endOfData();
            }

        }
    }

    private class SlicedEntry implements Entry<K, V> {

        private final K key;
        private final V value;

        public SlicedEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

    }
}
