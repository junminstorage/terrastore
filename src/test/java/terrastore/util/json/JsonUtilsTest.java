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
package terrastore.util.json;

import org.codehaus.jackson.map.ObjectMapper;
import terrastore.store.ValidationException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import terrastore.common.ClusterStats;
import terrastore.common.ErrorMessage;
import terrastore.server.Buckets;
import terrastore.server.Keys;
import terrastore.server.MapReduceDescriptor;
import terrastore.server.Parameters;
import terrastore.server.Values;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.util.collect.Sets;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class JsonUtilsTest {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    //
    private static final String JSON_KEYS = "[\"key1\",\"key2\",\"key3\"]";
    private static final String JSON_VALUE = "{\"key\":\"value\","
            + "\"array\":[\"primitive\",{\"nested\":[\"array\"]}],"
            + "\"object\":{\"inner\":\"value\"}}";
    private static final String JSON_VALUE_WITH_REPLACED_VALUE = "{\"key\":\"update\","
            + "\"array\":[\"primitive\",{\"nested\":[\"array\"]}],"
            + "\"object\":{\"inner\":\"value\"}}";
    private static final String JSON_VALUE_WITH_REMOVED_VALUE = "{"
            + "\"array\":[\"primitive\",{\"nested\":[\"array\"]}],"
            + "\"object\":{\"inner\":\"value\"}}";
    private static final String JSON_VALUE_WITH_VALUE_ADDED_TO_ARRAY = "{\"key\":\"value\","
            + "\"array\":[\"primitive\",{\"nested\":[\"array\"]},\"update\"],"
            + "\"object\":{\"inner\":\"value\"}}";
    private static final String JSON_VALUE_WITH_VALUE_REMOVED_FROM_ARRAY = "{\"key\":\"value\","
            + "\"array\":[{\"nested\":[\"array\"]}],"
            + "\"object\":{\"inner\":\"value\"}}";
    private static final String JSON_VALUE_WITH_VALUE_ADDED_TO_THIS_OBJECT = "{\"key\":\"value\","
            + "\"array\":[\"primitive\",{\"nested\":[\"array\"]}],"
            + "\"object\":{\"inner\":\"value\"},\"new\":\"value\"}";
    private static final String JSON_VALUE_WITH_VALUE_ADDED_TO_NESTED_OBJECT = "{\"key\":\"value\","
            + "\"array\":[\"primitive\",{\"nested\":[\"array\"]}],"
            + "\"object\":{\"inner\":\"value\",\"inner2\":\"value2\"}}";
    private static final String ARRAY_JSON_VALUE = "[\"test1\"]";
    private static final String BAD_JSON_VALUE = "{\"key\" : \"value\", "
            + "\"array\" : [\"primitive\", {\"nested\":[\"array\"]}], "
            + "\"key\" : {\"bad\":value}}";
    private static final String SIMPLE_JSON_VALUE = "{\"key\":\"value\"}";
    private static final String ERROR_MESSAGE = "{\"message\":\"test\",\"code\":0}";
    private static final String CLUSTER_STATS = "{\"clusters\":[{\"name\":\"cluster-1\",\"status\":\"AVAILABLE\",\"nodes\":[{\"name\":\"node-1\",\"host\":\"localhost\",\"port\":8080}]}]}";
    private static final String VALUES = "{\"value\":{\"key\":\"value\"}}";
    private static final String PARAMETERS = "{\"key\":\"value\"}";
    private static final String BUCKETS = "[\"1\",\"2\"]";
    private static final String MAPREDUCE_DESCRIPTOR = "{\"range\":{\"startKey\":\"k1\",\"timeToLive\":10000},\"task\":{\"mapper\":\"mapper\",\"reducer\":\"reducer\",\"timeout\":10000}}";

    @Test
    public void testValidate() throws Exception {
        Value json = new Value(JSON_VALUE.getBytes("UTF-8"));
        JsonUtils.validate(json);
    }

    @Test(expected = ValidationException.class)
    public void testValidateWithArrayJson() throws Exception {
        Value json = new Value(ARRAY_JSON_VALUE.getBytes("UTF-8"));
        JsonUtils.validate(json);
    }

    @Test(expected = ValidationException.class)
    public void testValidateWithBadJson() throws Exception {
        Value json = new Value(BAD_JSON_VALUE.getBytes("UTF-8"));
        JsonUtils.validate(json);
    }

    @Test
    public void testMergeWithReplacedValue() throws Exception {
        Value json = new Value(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> update = JSON_MAPPER.readValue("{\"*\":{\"key\":\"update\"}}", Map.class);
        assertEquals(JSON_VALUE_WITH_REPLACED_VALUE, new String(JsonUtils.merge(json, update).getBytes()));
    }

    @Test
    public void testMergeWithRemovedValue() throws Exception {
        Value json = new Value(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> update = JSON_MAPPER.readValue("{\"-\":[\"key\"]}", Map.class);
        assertEquals(JSON_VALUE_WITH_REMOVED_VALUE, new String(JsonUtils.merge(json, update).getBytes()));
    }

    @Test
    public void testMergeWithValueAddedToArray() throws Exception {
        Value json = new Value(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> update = JSON_MAPPER.readValue("{\"array\":[\"+\",\"update\"]}", Map.class);
        assertEquals(JSON_VALUE_WITH_VALUE_ADDED_TO_ARRAY, new String(JsonUtils.merge(json, update).getBytes()));
    }

    @Test
    public void testMergeWithValueRemovedFromArray() throws Exception {
        Value json = new Value(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> update = JSON_MAPPER.readValue("{\"array\":[\"-\",\"primitive\"]}", Map.class);
        assertEquals(JSON_VALUE_WITH_VALUE_REMOVED_FROM_ARRAY, new String(JsonUtils.merge(json, update).getBytes()));
    }

    @Test
    public void testMergeWithValueAddedToThisObject() throws Exception {
        Value json = new Value(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> update = JSON_MAPPER.readValue("{\"+\":{\"new\":\"value\"}}", Map.class);
        assertEquals(JSON_VALUE_WITH_VALUE_ADDED_TO_THIS_OBJECT, new String(JsonUtils.merge(json, update).getBytes()));
    }

    @Test
    public void testMergeWithValueAddedToNestedObject() throws Exception {
        Value json = new Value(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> update = JSON_MAPPER.readValue("{\"object\":{\"+\":{\"inner2\":\"value2\"}}}", Map.class);
        assertEquals(JSON_VALUE_WITH_VALUE_ADDED_TO_NESTED_OBJECT, new String(JsonUtils.merge(json, update).getBytes()));
    }

    @Test
    public void testToModifiableMap() throws Exception {
        Value json = new Value(SIMPLE_JSON_VALUE.getBytes("UTF-8"));
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
        Value json = new Value(SIMPLE_JSON_VALUE.getBytes("UTF-8"));
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
        Value value = JsonUtils.fromMap(map);
        assertArrayEquals(SIMPLE_JSON_VALUE.getBytes("UTF-8"), value.getBytes());
    }

    @Test
    public void testWriteClusterStats() throws Exception {
        ClusterStats.Cluster cluster = new ClusterStats.Cluster("cluster-1", Sets.linked(new ClusterStats.Node("node-1", "localhost", 8080)));
        ClusterStats clusterStats = new ClusterStats(Sets.linked(cluster));
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
    public void testWriteKeys() throws Exception {
        Keys keysToWrite = new Keys(Sets.linked(new Key("key1"), new Key("key2"), new Key("key3")));
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonUtils.write(keysToWrite, stream);
        assertEquals(JSON_KEYS, new String(stream.toByteArray()));
    }

    @Test
    public void testWriteValues() throws Exception {
        Value value = new Value(SIMPLE_JSON_VALUE.getBytes("UTF-8"));
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
        Parameters params = JsonUtils.readParameters(stream);
        assertEquals(1, params.size());
        assertEquals("key", params.keySet().toArray()[0]);
        assertEquals("value", params.values().toArray()[0]);
    }

    @Test
    public void testReadMapReduceDescriptor() throws Exception {
        ByteArrayInputStream stream = new ByteArrayInputStream(MAPREDUCE_DESCRIPTOR.getBytes("UTF-8"));
        MapReduceDescriptor descriptor = JsonUtils.readMapReduceDescriptor(stream);
        assertEquals(new Key("k1"), descriptor.range.startKey);
        assertEquals(10000, descriptor.range.timeToLive);
        assertEquals("mapper", descriptor.task.mapper);
        assertEquals("reducer", descriptor.task.reducer);
        assertEquals(10000, descriptor.task.timeout);
    }

}
