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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class CompositeMapTest {

    @Test
    public void testGet() {
        Set<String> keys = new LinkedHashSet<String>();
        keys.add("k1");
        keys.add("k2");
        keys.add("k3");
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("k1", "v1");
        Map<String, String> map2 = new HashMap<String, String>();
        map1.put("k2", "v2");
        Map<String, String> map3 = new HashMap<String, String>();
        map1.put("k3", "v3");
        List<Map<String, String>> values = new LinkedList<Map<String, String>>();
        values.add(map1);
        values.add(map2);
        values.add(map3);

        CompositeMap<String, String> composite = new CompositeMap<String, String>(keys, values);
        assertEquals("v1", composite.get("k1"));
        assertEquals("v2", composite.get("k2"));
        assertEquals("v3", composite.get("k3"));
    }

    @Test
    public void testGetWithNonExistentValueIsLenient() {
        Set<String> keys = new LinkedHashSet<String>();
        keys.add("k1");
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("k2", "v2");
        List<Map<String, String>> values = new LinkedList<Map<String, String>>();
        values.add(map1);

        CompositeMap<String, String> composite = new CompositeMap<String, String>(keys, values);
        assertNull(composite.get("k1"));
    }

    @Test
    public void testIteration() {
        Set<String> keys = new LinkedHashSet<String>();
        keys.add("k1");
        keys.add("k2");
        keys.add("k3");
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("k1", "v1");
        Map<String, String> map2 = new HashMap<String, String>();
        map1.put("k2", "v2");
        Map<String, String> map3 = new HashMap<String, String>();
        map1.put("k3", "v3");
        List<Map<String, String>> values = new LinkedList<Map<String, String>>();
        values.add(map1);
        values.add(map2);
        values.add(map3);

        CompositeMap<String, String> composite = new CompositeMap<String, String>(keys, values);
        int i = 1;
        for (Map.Entry<String, String> entry : composite.entrySet()) {
            assertEquals("k" + i, entry.getKey());
            assertEquals("v" + i, entry.getValue());
            i++;
        }
    }

    @Test
    public void testIterationWithNonExistentValuesIsLenient() {
        Set<String> keys = new LinkedHashSet<String>();
        keys.add("k1");
        keys.add("k2");
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("k1", "v1");
        List<Map<String, String>> values = new LinkedList<Map<String, String>>();
        values.add(map1);

        CompositeMap<String, String> composite = new CompositeMap<String, String>(keys, values);
        int i = 1;
        for (Map.Entry<String, String> entry : composite.entrySet()) {
            assertEquals("k" + i, entry.getKey());
            assertEquals("v" + i, entry.getValue());
            i++;
        }
        assertEquals(2, i);
    }

    @Test
    public void testSizeWithAllKeyValues() {
        Set<String> keys = new LinkedHashSet<String>();
        keys.add("k1");
        keys.add("k2");
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("k1", "v1");
        Map<String, String> map2 = new HashMap<String, String>();
        map1.put("k2", "v2");
        List<Map<String, String>> values = new LinkedList<Map<String, String>>();
        values.add(map1);
        values.add(map2);

        CompositeMap<String, String> composite = new CompositeMap<String, String>(keys, values);
        assertEquals(2, composite.size());
    }

    @Test
    public void testSizeWithMissingValues() {
        Set<String> keys = new LinkedHashSet<String>();
        keys.add("k1");
        keys.add("k2");
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("k1", "v1");
        List<Map<String, String>> values = new LinkedList<Map<String, String>>();
        values.add(map1);

        CompositeMap<String, String> composite = new CompositeMap<String, String>(keys, values);
        assertEquals(1, composite.size());
    }
}
