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
import terrastore.store.types.JsonValue;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class JsonStreamingListTest {

    private static final String EMPTY = "[]";
    //
    private static final String STRING = "[\"string\"]";
    private static final String INT = "[1]";
    private static final String FLOAT = "[1.5]";
    private static final String TRUE = "[true]";
    private static final String FALSE = "[false]";
    private static final String NULL = "[null]";
    private static final String OBJECT = "[{}]";
    private static final String ARRAY = "[[]]";
    //
    private static final String COMPOSITE = "[\"string\", 1, 1.5, true, false, null, {}, []]";
    //
    private static final String INNER_OBJECT = "[{\"key\":\"object\"}]";
    private static final String INNER_ARRAY = "[[\"array\"]]";

    @Test
    public void testEmptyArray() {
        JsonStreamingList list = new JsonStreamingList(new JsonValue(EMPTY.getBytes()));
        assertEquals(0, list.size());
    }

    @Test
    public void testTypes() {
        JsonStreamingList list = null;

        list = new JsonStreamingList(new JsonValue(STRING.getBytes()));
        assertEquals(1, list.size());
        assertEquals("string", list.get(0));

        list = new JsonStreamingList(new JsonValue(INT.getBytes()));
        assertEquals(1, list.size());
        assertEquals(1, list.get(0));

        list = new JsonStreamingList(new JsonValue(FLOAT.getBytes()));
        assertEquals(1, list.size());
        assertEquals(1.5f, list.get(0));

        list = new JsonStreamingList(new JsonValue(TRUE.getBytes()));
        assertEquals(1, list.size());
        assertEquals(true, list.get(0));

        list = new JsonStreamingList(new JsonValue(FALSE.getBytes()));
        assertEquals(1, list.size());
        assertEquals(false, list.get(0));

        list = new JsonStreamingList(new JsonValue(NULL.getBytes()));
        assertEquals(1, list.size());
        assertEquals(null, list.get(0));

        list = new JsonStreamingList(new JsonValue(OBJECT.getBytes()));
        assertEquals(1, list.size());
        assertTrue(JsonStreamingMap.class.isAssignableFrom(list.get(0).getClass()));

        list = new JsonStreamingList(new JsonValue(ARRAY.getBytes()));
        assertEquals(1, list.size());
        assertTrue(JsonStreamingList.class.isAssignableFrom(list.get(0).getClass()));
    }

    @Test
    public void testCompositeArray() {
        JsonStreamingList list = new JsonStreamingList(new JsonValue(COMPOSITE.getBytes()));
        assertEquals(8, list.size());
        assertEquals("string", list.get(0));
        assertEquals(1, list.get(1));
        assertEquals(1.5f, list.get(2));
        assertEquals(true, list.get(3));
        assertEquals(false, list.get(4));
        assertEquals(null, list.get(5));
        assertTrue(JsonStreamingMap.class.isAssignableFrom(list.get(6).getClass()));
        assertTrue(JsonStreamingList.class.isAssignableFrom(list.get(7).getClass()));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testArrayIndexOutOfBoundsException() {
        JsonStreamingList list = new JsonStreamingList(new JsonValue(COMPOSITE.getBytes()));
        assertEquals(8, list.size());
        list.get(8);
    }

    @Test
    public void testWithInnerObject() {
        JsonStreamingList list = new JsonStreamingList(new JsonValue(INNER_OBJECT.getBytes()));
        JsonStreamingMap inner = (JsonStreamingMap) list.get(0);
        assertEquals(1, list.size());
        assertEquals(1, inner.size());
        assertEquals("object", inner.get("key"));
    }

    @Test
    public void testWithInnerArray() {
        JsonStreamingList list = new JsonStreamingList(new JsonValue(INNER_ARRAY.getBytes()));
        JsonStreamingList inner = (JsonStreamingList) list.get(0);
        assertEquals(1, list.size());
        assertEquals(1, inner.size());
        assertEquals("array", inner.get(0));
    }
}
