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

import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang.time.StopWatch;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class MergeSetTest {

    @Test
    public void testMergeWithEmptySets() {
        Set<String> result = Sets.merge(new LinkedHashSet<String>(), new LinkedHashSet<String>());
        assertEquals(new LinkedHashSet<String>(), result);

        result = Sets.merge(Sets.linked("1"), new LinkedHashSet<String>());
        assertEquals(Sets.linked("1"), result);

        result = Sets.merge(new LinkedHashSet<String>(), Sets.linked("1"));
        assertEquals(Sets.linked("1"), result);
    }

    @Test
    public void testMergeWithEqualSets() {
        Set<String> result = Sets.merge(Sets.linked("1", "2"), Sets.linked("1", "2"));
        assertEquals(Sets.linked("1", "2"), result);
    }

    @Test
    public void testMergeWithDifferentSets() {
        Set<String> result = Sets.merge(Sets.linked("1", "2"), Sets.linked("3", "4"));
        assertEquals(Sets.linked("1", "2", "3", "4"), result);

        result = Sets.merge(Sets.linked("1", "2"), Sets.linked("2", "3"));
        assertEquals(Sets.linked("1", "2", "3"), result);

        result = Sets.merge(Sets.linked("1", "1"), Sets.linked("2", "2"));
        assertEquals(Sets.linked("1", "2"), result);

        result = Sets.merge(Sets.linked("1", "2"), Sets.linked("1", "3"));
        assertEquals(Sets.linked("1", "2", "3"), result);

        result = Sets.merge(Sets.linked("1", "2", "3"), Sets.linked("4", "5"));
        assertEquals(Sets.linked("1", "2", "3", "4", "5"), result);

        result = Sets.merge(Sets.linked("1", "3", "5"), Sets.linked("2", "4"));
        assertEquals(Sets.linked("1", "2", "3", "4", "5"), result);

        result = Sets.merge(Sets.linked("4", "5"), Sets.linked("1", "2", "3"));
        assertEquals(Sets.linked("1", "2", "3", "4", "5"), result);

        result = Sets.merge(Sets.linked("2", "4"), Sets.linked("1", "3", "5"));
        assertEquals(Sets.linked("1", "2", "3", "4", "5"), result);
    }

    @Test
    public void testPerf() {
        Set<String> first = new TreeSet<String>();
        Random r1 = new Random(System.nanoTime());
        for (int i = 0; i < 100000; i++) {
            first.add("" + r1.nextLong());
        }

        Set<String> second = new TreeSet<String>();
        Random r2 = new Random(System.nanoTime());
        for (int i = 0; i < 100000; i++) {
            second.add("" + r2.nextLong());
        }
        
        StopWatch sw = new StopWatch();
        sw.start();
        Sets.merge(first, second);
        sw.stop();
        System.out.println("Elapsed time in millis: " + sw.getTime());
    }
}
