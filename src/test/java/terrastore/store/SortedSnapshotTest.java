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

import java.io.File;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.time.StopWatch;
import org.junit.Before;
import org.junit.Test;
import terrastore.startup.Constants;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class SortedSnapshotTest {

    @Before
    public void setUp() {
        System.setProperty(Constants.TERRASTORE_HOME, System.getProperty("java.io.tmpdir"));
        new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + Constants.SNAPSHOTS_DIR).mkdir();
    }

    @Test
    public void testKeysInRange() {
        Set<Key> keys = new HashSet<Key>();
        keys.add(new Key("v"));
        keys.add(new Key("a"));
        keys.add(new Key("c"));
        keys.add(new Key("b"));

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<Key> sorted = snapshot.keysInRange(new Key("b"), new Key("c"), 0);
        assertEquals(2, sorted.size());
        assertEquals(new Key("b"), sorted.toArray()[0]);
        assertEquals(new Key("c"), sorted.toArray()[1]);
        snapshot.discard();
    }

    @Test
    public void testFullRange() {
        Set<Key> keys = new HashSet<Key>();
        keys.add(new Key("v"));
        keys.add(new Key("a"));
        keys.add(new Key("c"));
        keys.add(new Key("b"));

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<Key> sorted = snapshot.keysInRange(new Key("a"), new Key("v"), 0);
        assertEquals(4, sorted.size());
        assertEquals(new Key("a"), sorted.toArray()[0]);
        assertEquals(new Key("b"), sorted.toArray()[1]);
        assertEquals(new Key("c"), sorted.toArray()[2]);
        assertEquals(new Key("v"), sorted.toArray()[3]);
        snapshot.discard();
    }

    @Test
    public void testKeysInRangeWithStartOnly() {
        Set<Key> keys = new HashSet<Key>();
        keys.add(new Key("v"));
        keys.add(new Key("a"));
        keys.add(new Key("c"));
        keys.add(new Key("b"));

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<Key> sorted = snapshot.keysInRange(new Key("c"), null, 0);
        assertEquals(2, sorted.size());
        assertEquals(new Key("c"), sorted.toArray()[0]);
        assertEquals(new Key("v"), sorted.toArray()[1]);
        snapshot.discard();
    }

    @Test
    public void testKeysInRangeWithLimit() {
        Set<Key> keys = new HashSet<Key>();
        keys.add(new Key("v"));
        keys.add(new Key("a"));
        keys.add(new Key("c"));
        keys.add(new Key("b"));

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<Key> sorted = snapshot.keysInRange(new Key("a"), new Key("c"), 2);
        assertEquals(2, sorted.size());
        assertEquals(new Key("a"), sorted.toArray()[0]);
        assertEquals(new Key("b"), sorted.toArray()[1]);
        snapshot.discard();
    }

    @Test
    public void testUpdateWithMoreKeys() {
        Set<Key> keys = new HashSet<Key>();
        keys.add(new Key("d"));
        keys.add(new Key("a"));
        keys.add(new Key("c"));
        keys.add(new Key("b"));

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<Key> sorted = snapshot.keysInRange(new Key("a"), new Key("z"), 0);
        assertEquals(4, sorted.size());

        keys.add(new Key("e"));
        snapshot.update(keys);
        sorted = snapshot.keysInRange(new Key("a"), new Key("z"), 0);
        assertEquals(5, sorted.size());
        assertEquals(new Key("a"), sorted.toArray()[0]);
        assertEquals(new Key("b"), sorted.toArray()[1]);
        assertEquals(new Key("c"), sorted.toArray()[2]);
        assertEquals(new Key("d"), sorted.toArray()[3]);
        assertEquals(new Key("e"), sorted.toArray()[4]);

        snapshot.discard();
    }

    @Test
    public void testUpdateWithLessKeys() {
        Set<Key> keys = new HashSet<Key>();
        keys.add(new Key("d"));
        keys.add(new Key("a"));
        keys.add(new Key("c"));
        keys.add(new Key("b"));

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<Key> sorted = snapshot.keysInRange(new Key("a"), new Key("z"), 0);
        assertEquals(4, sorted.size());

        keys.remove(new Key("d"));
        snapshot.update(keys);
        sorted = snapshot.keysInRange(new Key("a"), new Key("z"), 0);
        assertEquals(3, sorted.size());
        assertEquals(new Key("a"), sorted.toArray()[0]);
        assertEquals(new Key("b"), sorted.toArray()[1]);
        assertEquals(new Key("c"), sorted.toArray()[2]);

        snapshot.discard();
    }

    @Test
    public void testUpdateWithEqualNumberButDifferentKeys() {
        Set<Key> keys = new HashSet<Key>();
        keys.add(new Key("d"));
        keys.add(new Key("a"));
        keys.add(new Key("c"));
        keys.add(new Key("b"));

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<Key> sorted = snapshot.keysInRange(new Key("a"), new Key("z"), 0);
        assertEquals(4, sorted.size());

        keys.remove(new Key("a"));
        keys.remove(new Key("b"));
        keys.remove(new Key("c"));
        keys.remove(new Key("d"));
        keys.add(new Key("e"));
        keys.add(new Key("f"));
        keys.add(new Key("g"));
        keys.add(new Key("h"));
        snapshot.update(keys);
        sorted = snapshot.keysInRange(new Key("a"), new Key("z"), 0);
        assertEquals(4, sorted.size());
        assertEquals(new Key("e"), sorted.toArray()[0]);
        assertEquals(new Key("f"), sorted.toArray()[1]);
        assertEquals(new Key("g"), sorted.toArray()[2]);
        assertEquals(new Key("h"), sorted.toArray()[3]);

        snapshot.discard();
    }

    @Test
    public void testUpdateWithDifferentNumberAndDifferentKeys() {
        Set<Key> keys = new HashSet<Key>();
        keys.add(new Key("d"));
        keys.add(new Key("a"));
        keys.add(new Key("c"));
        keys.add(new Key("b"));

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Set<Key> sorted = snapshot.keysInRange(new Key("a"), new Key("z"), 0);
        assertEquals(4, sorted.size());

        keys.remove(new Key("a"));
        keys.remove(new Key("b"));
        keys.remove(new Key("c"));
        keys.remove(new Key("d"));
        keys.add(new Key("z"));
        snapshot.update(keys);
        sorted = snapshot.keysInRange(new Key("a"), new Key("z"), 0);
        assertEquals(1, sorted.size());
        assertEquals(new Key("z"), sorted.toArray()[0]);

        snapshot.discard();
    }

    @Test
    public void testIsExpired() throws InterruptedException {
        Set<Key> keys = new HashSet<Key>();

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Thread.sleep(1000);
        assertTrue(snapshot.isExpired(500));
        snapshot.discard();
    }

    @Test
    public void testIsNotExpired() throws InterruptedException {
        Set<Key> keys = new HashSet<Key>();

        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new StringComparator());
        Thread.sleep(1000);
        assertFalse(snapshot.isExpired(5000));
        snapshot.discard();
    }

    @Test
    public void testPerf() {
        Set<Key> keys = new HashSet<Key>();
        int total = 100000;

        for (int i = 0; i < total; i++) {
            keys.add(new Key("" + i));
        }

        StopWatch indexing = new StopWatch();
        indexing.start();
        SortedSnapshot snapshot = new SortedSnapshot("bucket", keys, new LongStringComparator());
        indexing.stop();
        System.out.println("Indexing: " + indexing.getTime());

        StopWatch querying = new StopWatch();
        querying.start();
        Set<Key> sorted = snapshot.keysInRange(new Key("100"), new Key("102"), 2);
        querying.stop();
        assertEquals(2, sorted.size());
        assertEquals(new Key("100"), sorted.toArray()[0]);
        assertEquals(new Key("101"), sorted.toArray()[1]);
        System.out.println("Querying: " + querying.getTime());

        for (int i = 10; i < 110; i++) {
            keys.remove(new Key("" + i));
        }
        for (int i = total; i < total + 10000; i++) {
            keys.add(new Key("" + i));
        }

        StopWatch updating = new StopWatch();
        updating.start();
        snapshot.update(keys);
        updating.stop();
        System.out.println("Updating: " + updating.getTime());

        StopWatch querying2 = new StopWatch();
        querying2.start();
        sorted = snapshot.keysInRange(new Key("100"), new Key("102"), 2);
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
