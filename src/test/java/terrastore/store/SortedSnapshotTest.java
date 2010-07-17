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
package terrastore.store;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.time.StopWatch;
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

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<String> sorted = snapshot.keysInRange("b", "c", 0);
        assertEquals(2, sorted.size());
        assertEquals("b", sorted.toArray()[0]);
        assertEquals("c", sorted.toArray()[1]);
        snapshot.discard();
    }

    @Test
    public void testKeysInRangeWithStartOnly() {
        Set<String> keys = new HashSet<String>();
        keys.add("v");
        keys.add("a");
        keys.add("c");
        keys.add("b");

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<String> sorted = snapshot.keysInRange("c", null, 0);
        assertEquals(2, sorted.size());
        assertEquals("c", sorted.toArray()[0]);
        assertEquals("v", sorted.toArray()[1]);
        snapshot.discard();
    }

    @Test
    public void testKeysInRangeWithLimit() {
        Set<String> keys = new HashSet<String>();
        keys.add("v");
        keys.add("a");
        keys.add("c");
        keys.add("b");

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<String> sorted = snapshot.keysInRange("a", "c", 2);
        assertEquals(2, sorted.size());
        assertEquals("a", sorted.toArray()[0]);
        assertEquals("b", sorted.toArray()[1]);
        snapshot.discard();
    }

    @Test
    public void testUpdateWithMoreKeys() {
        Set<String> keys = new HashSet<String>();
        keys.add("d");
        keys.add("a");
        keys.add("c");
        keys.add("b");

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<String> sorted = snapshot.keysInRange("a", "z", 0);
        assertEquals(4, sorted.size());

        keys.add("e");
        snapshot.update(keys);
        sorted = snapshot.keysInRange("a", "z", 0);
        assertEquals(5, sorted.size());
        assertEquals("a", sorted.toArray()[0]);
        assertEquals("b", sorted.toArray()[1]);
        assertEquals("c", sorted.toArray()[2]);
        assertEquals("d", sorted.toArray()[3]);
        assertEquals("e", sorted.toArray()[4]);

        snapshot.discard();
    }

    @Test
    public void testUpdateWithLessKeys() {
        Set<String> keys = new HashSet<String>();
        keys.add("d");
        keys.add("a");
        keys.add("c");
        keys.add("b");

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<String> sorted = snapshot.keysInRange("a", "z", 0);
        assertEquals(4, sorted.size());

        keys.remove("d");
        snapshot.update(keys);
        sorted = snapshot.keysInRange("a", "z", 0);
        assertEquals(3, sorted.size());
        assertEquals("a", sorted.toArray()[0]);
        assertEquals("b", sorted.toArray()[1]);
        assertEquals("c", sorted.toArray()[2]);

        snapshot.discard();
    }

    @Test
    public void testUpdateWithEqualNumberButDifferentKeys() {
        Set<String> keys = new HashSet<String>();
        keys.add("d");
        keys.add("a");
        keys.add("c");
        keys.add("b");

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<String> sorted = snapshot.keysInRange("a", "z", 0);
        assertEquals(4, sorted.size());

        keys.remove("a");
        keys.remove("b");
        keys.remove("c");
        keys.remove("d");
        keys.add("e");
        keys.add("f");
        keys.add("g");
        keys.add("h");
        snapshot.update(keys);
        sorted = snapshot.keysInRange("a", "z", 0);
        assertEquals(4, sorted.size());
        assertEquals("e", sorted.toArray()[0]);
        assertEquals("f", sorted.toArray()[1]);
        assertEquals("g", sorted.toArray()[2]);
        assertEquals("h", sorted.toArray()[3]);

        snapshot.discard();
    }

    @Test
    public void testUpdateWithDifferentNumberAndDifferentKeys() {
        Set<String> keys = new HashSet<String>();
        keys.add("d");
        keys.add("a");
        keys.add("c");
        keys.add("b");

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<String> sorted = snapshot.keysInRange("a", "z", 0);
        assertEquals(4, sorted.size());

        keys.remove("a");
        keys.remove("b");
        keys.remove("c");
        keys.remove("d");
        keys.add("z");
        snapshot.update(keys);
        sorted = snapshot.keysInRange("a", "z", 0);
        assertEquals(1, sorted.size());
        assertEquals("z", sorted.toArray()[0]);

        snapshot.discard();
    }

    @Test
    public void testIsExpired() throws InterruptedException {
        Set<String> keys = new HashSet<String>();

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Thread.sleep(1000);
        assertTrue(snapshot.isExpired(500));
        snapshot.discard();
    }

    @Test
    public void testIsNotExpired() throws InterruptedException {
        Set<String> keys = new HashSet<String>();

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Thread.sleep(1000);
        assertFalse(snapshot.isExpired(5000));
        snapshot.discard();
    }

    @Test
    public void testPerf() {
        Set<String> keys = new HashSet<String>();
        int total = 300000;

        for (int i = 0; i < total; i++) {
            keys.add("" + i);
        }

        StopWatch indexing = new StopWatch();
        indexing.start();
        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new LongStringComparator());
        indexing.stop();
        System.out.println("Indexing: " + indexing.getTime());

        StopWatch querying = new StopWatch();
        querying.start();
        Set<String> sorted = snapshot.keysInRange("100", "102", 2);
        querying.stop();
        assertEquals(2, sorted.size());
        assertEquals("100", sorted.toArray()[0]);
        assertEquals("101", sorted.toArray()[1]);
        System.out.println("Querying: " + querying.getTime());

        for (int i = 10; i < 110; i++) {
            keys.remove("" + i);
        }
        for (int i = total; i < total + 10000; i++) {
            keys.add("" + i);
        }

        StopWatch updating = new StopWatch();
        updating.start();
        snapshot.update(keys);
        updating.stop();
        System.out.println("Updating: " + updating.getTime());

        StopWatch querying2 = new StopWatch();
        querying2.start();
        sorted = snapshot.keysInRange("100", "102", 2);
        querying2.stop();
        assertEquals(0, sorted.size());
        System.out.println("Querying 2: " + querying2.getTime());

        snapshot.discard();
    }

    private static class StringComparator implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }

    private static class LongStringComparator implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            return Long.valueOf(o1).compareTo(Long.valueOf(o2));
        }
    }
}
