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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import terrastore.common.ClusterStats;
import terrastore.common.ErrorMessage;
import terrastore.server.Buckets;
import terrastore.server.Parameters;
import terrastore.server.Values;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.store.types.JsonValue;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class JsonUtilsTest {

    private static final String JSON_VALUE = "{\"key\" : \"value\", "
            + "\"array\" : [\"primitive\", {\"nested\":[\"array\"]}], "
            + "\"key\" : {\"object\":\"value\"}}";
    private static final String BAD_JSON_VALUE = "{\"key\" : \"value\", "
            + "\"array\" : [\"primitive\", {\"nested\":[\"array\"]}], "
            + "\"key\" : {\"bad\":value}}";
    private static final String SIMPLE_JSON_VALUE = "{\"key\":\"value\"}";
    private static final String ERROR_MESSAGE = "{\"message\":\"test\",\"code\":0}";
    private static final String CLUSTER_STATS = "{\"clusters\":[{\"name\":\"cluster-1\",\"nodes\":[{\"name\":\"node-1\",\"host\":\"localhost\",\"port\":8080}]}]}";
    private static final String VALUES = "{\"value\":{\"key\":\"value\"}}";
    private static final String PARAMETERS = "{\"key\":\"value\"}";
    private static final String BUCKETS = "[\"1\",\"2\"]";

    @Test
    public void testValidate() throws Exception {
        JsonValue json = new JsonValue(JSON_VALUE.getBytes("UTF-8"));
        JsonUtils.validate(json);
    }

    @Test(expected = IOException.class)
    public void testValidateWithBadJson() throws Exception {
        JsonValue json = new JsonValue(BAD_JSON_VALUE.getBytes("UTF-8"));
        JsonUtils.validate(json);
    }

    @Test
    public void testToModifiableMap() throws Exception {
        JsonValue json = new JsonValue(SIMPLE_JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> map = JsonUtils.toModifiableMap(json);
        assertNotNull(map);
        assertEquals(1, map.size());
        assertTrue(map.containsKey("key"));
        assertTrue(map.containsValue("value"));
        //
        assertFalse(JsonStreamingMap.class.equals(map.getClass()));
    }

    @Test
    public void testToUnmodifiableMap() throws Exception {
        JsonValue json = new JsonValue(SIMPLE_JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> map = JsonUtils.toUnmodifiableMap(json);
        assertNotNull(map);
        assertEquals(1, map.size());
        assertTrue(map.containsKey("key"));
        assertTrue(map.containsValue("value"));
        //
        assertTrue(JsonStreamingMap.class.equals(map.getClass()));
    }

    @Test
    public void testFromMap() throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("key", "value");
        JsonValue value = JsonUtils.fromMap(map);
        assertArrayEquals(SIMPLE_JSON_VALUE.getBytes("UTF-8"), value.getBytes());
    }

    @Test
    public void testWriteClusterStats() throws Exception {
        ClusterStats clusterStats = new ClusterStats();
        ClusterStats.Cluster c = new ClusterStats.Cluster("cluster-1");
        clusterStats.getClusters().add(c);
        c.getNodes().add(new ClusterStats.Node("node-1", "localhost", 8080));
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonUtils.write(clusterStats, stream);
        assertEquals(CLUSTER_STATS, new String(stream.toByteArray()));
    }

    @Test
    public void testWriteErrorMessage() throws Exception {
        ErrorMessage message = new ErrorMessage(0, "test");
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonUtils.write(message, stream);
        assertEquals(ERROR_MESSAGE, new String(stream.toByteArray()));
    }

    @Test
    public void testWriteValues() throws Exception {
        JsonValue value = new JsonValue(SIMPLE_JSON_VALUE.getBytes("UTF-8"));
        Map<Key, Value> map = new HashMap<Key, Value>();
        map.put(new Key("value"), value);
        Values values = new Values(map);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonUtils.write(values, stream);
        assertEquals(VALUES, new String(stream.toByteArray()));
    }

    @Test
    public void testWriteBuckets() throws Exception {
        Set<String> names = new LinkedHashSet<String>();
        names.add("1");
        names.add("2");
        Buckets buckets = new Buckets(names);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonUtils.write(buckets, stream);
        assertEquals(BUCKETS, new String(stream.toByteArray()));
    }

    @Test
    public void testReadParameters() throws Exception {
        ByteArrayInputStream stream = new ByteArrayInputStream(PARAMETERS.getBytes("UTF-8"));
        Parameters params = JsonUtils.read(stream);
        assertEquals(1, params.size());
        assertEquals("key", params.keySet().toArray()[0]);
        assertEquals("value", params.values().toArray()[0]);
    }
}
