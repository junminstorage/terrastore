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
package terrastore.store;

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Sorted snapshot of keys.
 *
 * @author Sergio Bossa
 */
public class SortedSnapshot {

    private final NavigableSet<String> sortedKeys;
    private final long timestamp;

    public SortedSnapshot(Set<String> keys, Comparator<String> comparator) {
        this.sortedKeys = new TreeSet<String>(comparator);
        this.timestamp = System.currentTimeMillis();
        this.sortedKeys.addAll(keys);
    }

    public SortedSet<String> keysInRange(String start, String end) {
        return sortedKeys.subSet(start, true, end, true);
    }

    public boolean isExpired(long timeToLive) {
        long now = System.currentTimeMillis();
        return (now - timestamp) >= timeToLive;
    }
}
