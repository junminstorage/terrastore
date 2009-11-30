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
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class SortedSnapshotTest {

    @Test
    public void testKeysInRange() {
        Set<String> keys = new HashSet<String>();
        keys.add("v");
        keys.add("a");
        keys.add("c");
        keys.add("b");

        SortedSnapshot snapshot = new SortedSnapshot(keys, new StringComparator());
        SortedSet<String> sorted = snapshot.keysInRange("b", "c");
        assertEquals(2, sorted.size());
        assertEquals("b", sorted.first());
        assertEquals("c", sorted.last());
    }

    @Test
    public void testIsExpired() throws InterruptedException {
        Set<String> keys = new HashSet<String>();

        SortedSnapshot snapshot = new SortedSnapshot(keys, new StringComparator());
        Thread.sleep(1000);
        assertTrue(snapshot.isExpired(500));
    }

    @Test
    public void testIsNotExpired() throws InterruptedException {
        Set<String> keys = new HashSet<String>();

        SortedSnapshot snapshot = new SortedSnapshot(keys, new StringComparator());
        Thread.sleep(1000);
        assertFalse(snapshot.isExpired(5000));
    }

    private static class StringComparator implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }
}