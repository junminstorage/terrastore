/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.util;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class DrainMap<K, V> extends AbstractMap<K, V> {

    private final Map<K, V> backend;

    DrainMap(List<Map<K, V>> maps, Map<K, V> backend) {
        this.backend = backend;
        for (Map<K, V> map : maps) {
            Iterator<Entry<K, V>> entries = map.entrySet().iterator();
            while (entries.hasNext()) {
                Entry<K, V> entry = entries.next();
                backend.put(entry.getKey(), entry.getValue());
                entries.remove();
            }
        }
    }

    @Override
    public V get(Object key) {
        return backend.get(key);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return backend.entrySet();
    }
}
