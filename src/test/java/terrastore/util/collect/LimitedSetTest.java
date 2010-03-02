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

import java.util.Set;
import java.util.TreeSet;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class LimitedSetTest {

    @Test
    public void testWithLimit() {
        Set<String> source = new TreeSet<String>(com.google.common.collect.Sets.newHashSet("a", "b", "c"));
        Set<String> limited = new LimitedSet<String>(source, 2);
        assertEquals(2, limited.size());
        assertEquals("a", limited.toArray()[0]);
        assertEquals("b", limited.toArray()[1]);
    }

    @Test
    public void testWithNoLimit() {
        Set<String> source = new TreeSet<String>(com.google.common.collect.Sets.newHashSet("a", "b", "c"));
        Set<String> limited = new LimitedSet<String>(source, 0);
        assertEquals(3, limited.size());
        assertEquals("a", limited.toArray()[0]);
        assertEquals("b", limited.toArray()[1]);
        assertEquals("c", limited.toArray()[2]);
    }
}