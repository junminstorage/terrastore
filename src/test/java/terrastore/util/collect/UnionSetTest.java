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

import java.util.Arrays;
import java.util.Set;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class UnionSetTest {

    @Test
    public void testSizeWithOneMap() {
        Set<String> set1 = com.google.common.collect.Sets.newHashSet("1", "2");
        UnionSet<String> union = new UnionSet<String>(Arrays.asList(set1));
        assertEquals(2, union.size());
    }

    @Test
    public void testSizeWithMoreMaps() {
        Set<String> set1 = com.google.common.collect.Sets.newHashSet("1", "2");
        Set<String> set2 = com.google.common.collect.Sets.newHashSet("3", "4");
        UnionSet<String> union = new UnionSet<String>(Arrays.asList(set1, set2));
        assertEquals(4, union.size());
    }

    @Test
    public void testIterationWithOneMap() {
        Set<String> set1 = com.google.common.collect.Sets.newLinkedHashSet(Arrays.asList("1", "2"));
        UnionSet<String> union = new UnionSet<String>(Arrays.asList(set1));
        int i = 1;
        for (String s : union) {
            assertEquals("" + i++, s);
        }
        assertEquals(3, i);
    }

    @Test
    public void testIterationWithMoreMaps() {
        Set<String> set1 = com.google.common.collect.Sets.newLinkedHashSet(Arrays.asList("1", "2"));
        Set<String> set2 = com.google.common.collect.Sets.newLinkedHashSet(Arrays.asList("3", "4"));
        UnionSet<String> union = new UnionSet<String>(Arrays.asList(set1, set2));
        int i = 1;
        for (String s : union) {
            assertEquals("" + i++, s);
        }
        assertEquals(5, i);
    }

    @Test
    public void testDuplicatesAreIgnored() {
        Set<String> set1 = com.google.common.collect.Sets.newLinkedHashSet(Arrays.asList("1", "2"));
        Set<String> set2 = com.google.common.collect.Sets.newLinkedHashSet(Arrays.asList("2", "3"));
        UnionSet<String> union = new UnionSet<String>(Arrays.asList(set1, set2));
        assertEquals(3, union.size());
        int i = 1;
        for (String s : union) {
            assertEquals("" + i++, s);
        }
        assertEquals(4, i);
    }
}