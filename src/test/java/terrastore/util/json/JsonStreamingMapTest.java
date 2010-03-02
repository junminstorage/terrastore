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
package terrastore.util.json;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class JsonStreamingMapTest {

    private static final String EMPTY = "{}";
    //
    private static final String STRING = "{\"key\":\"string\"}";
    private static final String INT = "{\"key\":1}";
    private static final String FLOAT = "{\"key\":1.5}";
    private static final String TRUE = "{\"key\":true}";
    private static final String FALSE = "{\"key\":false}";
    private static final String NULL = "{\"key\":null}";
    private static final String OBJECT = "{\"key\":{}}";
    private static final String ARRAY = "{\"key\":[]}";
    //
    private static final String COMPOSITE = "{\"key1\":\"string\", \"key2\":1, \"key3\":1.5, \"key4\":true, \"key5\":false, \"key6\":null, \"key7\":{}, \"key8\":[]}";
    //
    private static final String INNER_OBJECT = "{\"inner\":{\"key\":\"object\"}}";
    private static final String INNER_ARRAY = "{\"inner\":[\"array\"]}";

    @Test
    public void testEmptyMap() {
        JsonStreamingMap map = new JsonStreamingMap(EMPTY.getBytes());
        assertEquals(0, map.size());
    }

    @Test
    public void testTypes() {
        JsonStreamingMap map = null;

        map = new JsonStreamingMap(STRING.getBytes());
        assertEquals(1, map.size());
        assertEquals("string", map.get("key"));

        map = new JsonStreamingMap(INT.getBytes());
        assertEquals(1, map.size());
        assertEquals(1, map.get("key"));

        map = new JsonStreamingMap(FLOAT.getBytes());
        assertEquals(1, map.size());
        assertEquals(1.5f, map.get("key"));

        map = new JsonStreamingMap(TRUE.getBytes());
        assertEquals(1, map.size());
        assertEquals(true, map.get("key"));

        map = new JsonStreamingMap(FALSE.getBytes());
        assertEquals(1, map.size());
        assertEquals(false, map.get("key"));

        map = new JsonStreamingMap(NULL.getBytes());
        assertEquals(1, map.size());
        assertEquals(null, map.get("key"));

        map = new JsonStreamingMap(OBJECT.getBytes());
        assertEquals(1, map.size());
        assertTrue(JsonStreamingMap.class.isAssignableFrom(map.get("key").getClass()));

        map = new JsonStreamingMap(ARRAY.getBytes());
        assertEquals(1, map.size());
        assertTrue(JsonStreamingList.class.isAssignableFrom(map.get("key").getClass()));
    }

    @Test
    public void testCompositeMap() {
        JsonStreamingMap map = new JsonStreamingMap(COMPOSITE.getBytes());
        assertEquals(8, map.size());
        assertEquals("string", map.get("key1"));
        assertEquals(1, map.get("key2"));
        assertEquals(1.5f, map.get("key3"));
        assertEquals(true, map.get("key4"));
        assertEquals(false, map.get("key5"));
        assertEquals(null, map.get("key6"));
        assertTrue(JsonStreamingMap.class.isAssignableFrom(map.get("key7").getClass()));
        assertTrue(JsonStreamingList.class.isAssignableFrom(map.get("key8").getClass()));
    }

    @Test
    public void testWithInnerObject() {
        JsonStreamingMap map = new JsonStreamingMap(INNER_OBJECT.getBytes());
        JsonStreamingMap inner = (JsonStreamingMap) map.get("inner");
        assertEquals(1, map.size());
        assertEquals(1, inner.size());
        assertEquals("object", inner.get("key"));
    }

    @Test
    public void testWithInnerArray() {
        JsonStreamingMap map = new JsonStreamingMap(INNER_ARRAY.getBytes());
        JsonStreamingList inner = (JsonStreamingList) map.get("inner");
        assertEquals(1, map.size());
        assertEquals(1, inner.size());
        assertEquals("array", inner.get(0));
    }

    @Test
    public void testGetNullValue() {
        JsonStreamingMap map = new JsonStreamingMap(COMPOSITE.getBytes());
        assertNull(map.get("no.key"));
    }
}
