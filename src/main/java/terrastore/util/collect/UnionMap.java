/**
 * Copyright 2009 - 2010 Sergio Bossa (sergio.bossa@gmail.com)
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class UnionMap<K, V> extends AbstractMap<K, V> {

    private final List<Map<K, V>> maps;

    UnionMap(List<Map<K, V>> maps) {
        this.maps = new ArrayList<Map<K, V>>(maps);
    }

    @Override
    public V get(Object key) {
        V found = null;
        for (Map<K, V> map : maps) {
            found = map.get(key);
            if (found != null) {
                break;
            }
        }
        return found;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new UnionSet();
    }

    private class UnionSet extends AbstractSet<Entry<K, V>> {

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new UnionIterator();
        }

        @Override
        public int size() {
            UnionIterator iterator = new UnionIterator();
            int size = 0;
            while (iterator.hasNext()) {
                iterator.next();
                size++;
            }
            return size;
        }

        private class UnionIterator extends AbstractIterator<Entry<K, V>> {

            private int currentMapIndex = 0;
            private Set<K> computedKeys = new HashSet<K>();
            private Iterator<Entry<K, V>> currentMapIterator;

            @Override
            protected Entry<K, V> computeNext() {
                while (true) {
                    if (currentMapIterator == null || !currentMapIterator.hasNext()) {
                        currentMapIterator = null;
                        while (currentMapIndex < maps.size() && currentMapIterator == null) {
                            Map<K, V> currentMap = maps.get(currentMapIndex);
                            if (currentMap.size() > 0) {
                                currentMapIterator = currentMap.entrySet().iterator();
                            }
                            currentMapIndex++;
                        }
                    }
                    if (currentMapIterator != null && currentMapIterator.hasNext()) {
                        Entry<K, V> entry = currentMapIterator.next();
                        if (!computedKeys.contains(entry.getKey())) {
                            computedKeys.add(entry.getKey());
                            return entry;
                        }
                    } else {
                        return endOfData();
                    }
                }
            }
        }
    }
}
