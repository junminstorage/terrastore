/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.util.collect;

import com.google.common.collect.AbstractIterator;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class CompositeMap<K, V> extends AbstractMap<K, V> {

    private Set<K> keys;
    private final List<Map<K, V>> values;

    CompositeMap(Set<K> keys, List<Map<K, V>> values) {
        this.keys = keys;
        this.values = new ArrayList<Map<K, V>>(values);
    }

    @Override
    public V get(Object key) {
        if (keys.contains(key)) {
            return innerGet(key);
        } else {
            return null;
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new CompositeSet();
    }

    public V innerGet(Object key) {
        V found = null;
        for (Map<K, V> map : values) {
            found = map.get(key);
            if (found != null) {
                break;
            }
        }
        return found;
    }

    private class CompositeSet extends AbstractSet<Entry<K, V>> {

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new CompositeIterator();
        }

        @Override
        public int size() {
            int size = 0;
            for (Map<K, V> map : values) {
                size += map.size();
            }
            return size;
        }

        private class CompositeIterator extends AbstractIterator<Entry<K, V>> {

            private final Iterator<K> keysIterator = keys.iterator();

            @Override
            protected Entry<K, V> computeNext() {
                while (true) {
                    if (keysIterator.hasNext()) {
                        final K key = keysIterator.next();
                        final V value = innerGet(key);
                        if (value != null) {
                            return new Entry<K, V>() {

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
                                    throw new UnsupportedOperationException("This map is immutable.");
                                }
                            };
                        }
                    } else {
                        return endOfData();
                    }
                }
            }
        }
    }
}
