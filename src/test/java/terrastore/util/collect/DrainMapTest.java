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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Sergio Bossa
 */
public class DrainMapTest {

    private List<Map<String, String>> maps;

    @Test
    public void testGetWithOneMap() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("k1", "v1");
        map1.put("k2", "v2");
        maps = new LinkedList<Map<String, String>>();
        maps.add(map1);
        DrainMap<String, String> drain = new DrainMap<String, String>(maps, new HashMap<String, String>());
        assertEquals("v1", drain.get("k1"));
    }

    @Test
    public void testGetWithMoreMaps() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("k1", "v1");
        map1.put("k2", "v2");
        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("k3", "v3");
        map2.put("k4", "v4");
        maps = new LinkedList<Map<String, String>>();
        maps.add(map1);
        maps.add(map2);
        DrainMap<String, String> drain = new DrainMap<String, String>(maps, new HashMap<String, String>());
        assertEquals("v1", drain.get("k1"));
        assertEquals("v2", drain.get("k2"));
        assertEquals("v3", drain.get("k3"));
        assertEquals("v3", drain.get("k3"));
    }

    @Test
    public void testSizeWithOneMap() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("k1", "v1");
        map1.put("k2", "v2");
        maps = new LinkedList<Map<String, String>>();
        maps.add(map1);
        DrainMap<String, String> drain = new DrainMap<String, String>(maps, new HashMap<String, String>());
        assertEquals(2, drain.size());
    }

    @Test
    public void testSizeWithMoreMaps() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("k1", "v1");
        map1.put("k2", "v2");
        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("k3", "v3");
        map2.put("k4", "v4");
        maps = new LinkedList<Map<String, String>>();
        maps.add(map1);
        maps.add(map2);
        DrainMap<String, String> drain = new DrainMap<String, String>(maps, new HashMap<String, String>());
        assertEquals(4, drain.size());
    }

    @Test
    public void testEntriesWithOneMap() {
        Map<String, String> map1 = new LinkedHashMap<String, String>();
        map1.put("k1", "v1");
        map1.put("k2", "v2");
        maps = new LinkedList<Map<String, String>>();
        maps.add(map1);
        DrainMap<String, String> drain = new DrainMap<String, String>(maps, new HashMap<String, String>());
        int i = 0;
        for (Map.Entry<String, String> entry : drain.entrySet()) {
            i++;
            assertEquals("k" + i, entry.getKey());
        }
        assertEquals(2, i);
    }

    @Test
    public void testEntriesWithMoreMaps() {
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("k1", "v1");
        map1.put("k2", "v2");
        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("k3", "v3");
        map2.put("k4", "v4");
        maps = new LinkedList<Map<String, String>>();
        maps.add(map1);
        maps.add(map2);
        DrainMap<String, String> drain = new DrainMap<String, String>(maps, new LinkedHashMap<String, String>());
        int i = 0;
        for (Map.Entry<String, String> entry : drain.entrySet()) {
            i++;
            assertEquals("k" + i, entry.getKey());
        }
        assertEquals(4, i);
    }

    @Test
    public void testEntriesWithEmptyMap() {
        Map<String, String> map1 = new LinkedHashMap<String, String>();
        maps = new LinkedList<Map<String, String>>();
        maps.add(map1);
        DrainMap<String, String> drain = new DrainMap<String, String>(maps, new HashMap<String, String>());
        int i = 0;
        for (Map.Entry<String, String> entry : drain.entrySet()) {
            i++;
        }
        assertEquals(0, i);
    }
}