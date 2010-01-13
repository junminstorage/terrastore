/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import terrastore.store.Value;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class JsonUtilsTest {

    private static final String JSON_VALUE = "{\"key\" : \"value\", " +
            "\"array\" : [\"primitive\", {\"nested\":[\"array\"]}], " +
            "\"key\" : {\"object\":\"value\"}}";
    private static final String BAD_JSON_VALUE = "{\"key\" : \"value\", " +
            "\"array\" : [\"primitive\", {\"nested\":[\"array\"]}], " +
            "\"key\" : {\"bad\":value}}";
    private static final String SIMPLE_JSON_VALUE = "{\"key\":\"value\"}";

    @Test
    public void testValidate() throws Exception {
        Value json = new Value(JSON_VALUE.getBytes("UTF-8"));
        JsonUtils.validate(json);
    }

    @Test(expected = IOException.class)
    public void testValidateWithBadJson() throws Exception {
        Value json = new Value(BAD_JSON_VALUE.getBytes("UTF-8"));
        JsonUtils.validate(json);
    }

    @Test
    public void testToMap() throws Exception {
        Value json = new Value(SIMPLE_JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> map = JsonUtils.toMap(json);
        assertNotNull(map);
        assertEquals(1, map.size());
        assertTrue(map.containsKey("key"));
        assertTrue(map.containsValue("value"));
    }

    @Test
    public void testFromMap() throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("key", "value");
        Value value = JsonUtils.fromMap(map);
        assertArrayEquals(SIMPLE_JSON_VALUE.getBytes("UTF-8"), value.getBytes());
    }
}
