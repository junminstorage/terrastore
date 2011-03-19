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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class SlicedMapTest {

    @Test
    public void testSlice() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");
        Set<String> slice = new HashSet<String>();
        slice.add("k1");
        slice.add("k2");
        SlicedMap<String, String> slicedMap = new SlicedMap<String, String>(map, slice);
        assertEquals(2, slicedMap.size());
        assertEquals("v1", slicedMap.get("k1"));
        assertEquals("v2", slicedMap.get("k2"));
        assertNull(slicedMap.get("k3"));
    }

    @Test
    public void testSliceIteration() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");
        Set<String> slice = new HashSet<String>();
        slice.add("k1");
        SlicedMap<String, String> slicedMap = new SlicedMap<String, String>(map, slice);
        for (Map.Entry<String, String> entry : slicedMap.entrySet()) {
            assertEquals("k1", entry.getKey());
            assertEquals("v1", entry.getValue());
        }
    }

    @Test
    public void testSliceWithNonExistentKey() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");
        Set<String> slice = new HashSet<String>();
        slice.add("k4");
        SlicedMap<String, String> slicedMap = new SlicedMap<String, String>(map, slice);
        assertEquals(0, slicedMap.size());
        assertNull(slicedMap.get("k4"));
        for (Map.Entry<String, String> entry : slicedMap.entrySet()) {
            fail();
        }
    }

}
