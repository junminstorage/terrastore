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
package terrastore.store.impl;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import terrastore.event.ActionExecutor;
import terrastore.internal.tc.TCMaster;
import terrastore.event.Event;
import terrastore.event.EventBus;
import terrastore.event.EventListener;
import terrastore.startup.Constants;
import terrastore.store.Key;
import terrastore.store.StoreOperationException;
import terrastore.store.features.Predicate;
import terrastore.store.operators.Function;
import terrastore.store.features.Update;
import terrastore.store.features.Range;
import terrastore.store.operators.Condition;
import terrastore.store.Value;
import terrastore.store.features.Mapper;
import terrastore.store.operators.Comparator;
import terrastore.util.collect.Maps;
import terrastore.util.collect.Sets;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class TCBucketTest {

    private static final String JSON_VALUE = "{\"test\":\"test\"}";
    private static final String JSON_VALUE_2 = "{\"test2\":\"test2\"}";
    private static final String JSON_UPDATED = "{\"test\":\"value1\"}";
    private TCBucket bucket;

    @Before
    public void setUp() {
        TCMaster.getInstance().setupLocally();
        bucket = new TCBucket("bucket");
        bucket.setLockManager(new TCLockManager("test", 128));
        bucket.setSnapshotManager(new LocalSnapshotManager());
        bucket.setBackupManager(new DefaultBackupManager());
        bucket.setEventBus(new DisabledEventBus());
    }

    @Test()
    public void testPutAndClearAndSize() throws StoreOperationException {
        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());
        bucket.put(key, value);
        assertEquals(1, bucket.size());
        bucket.clear();
        assertEquals(0, bucket.size());
    }

    @Test
    public void testPutAndGetValue() throws StoreOperationException {
        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());
        bucket.put(key, value);
        assertEquals(value, bucket.get(key));
    }

    @Test
    public void testPutAndGetValues() throws StoreOperationException {
        Key key1 = new Key("key1");
        Key key2 = new Key("key2");
        Value value1 = new Value(JSON_VALUE.getBytes());
        Value value2 = new Value(JSON_VALUE_2.getBytes());
        bucket.put(key1, value1);
        bucket.put(key2, value2);
        Map<Key, Value> result = bucket.get(Sets.hash(key1, key2));
        assertEquals(2, result.size());
        assertTrue(result.containsKey(key1));
        assertTrue(result.containsKey(key2));
    }

    @Test
    public void testConditionalPutAlwaysWorkWithNoOldValue() throws StoreOperationException {
        final Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());
        Predicate predicate = new Predicate("test:unsatisfied");
        Condition condition = new Condition() {

            @Override
            public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
                if (value.get("test").equals(expression)) {
                    return true;
                } else {
                    return false;
                }
            }

        };

        bucket.setConditions(Maps.hash(new String[]{"test"}, new Condition[]{condition}));
        assertTrue(bucket.conditionalPut(key, value, predicate));
        assertEquals(value, bucket.get(key));
    }

    @Test
    public void testConditionalPutSuceedsBecauseSatisfied() throws StoreOperationException {
        final Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());
        Value updated = new Value(JSON_UPDATED.getBytes());

        bucket.put(key, value);

        Predicate predicate = new Predicate("test:test");
        Condition condition = new Condition() {

            @Override
            public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
                if (value.get("test").equals(expression)) {
                    return true;
                } else {
                    return false;
                }
            }

        };

        bucket.setConditions(Maps.hash(new String[]{"test"}, new Condition[]{condition}));
        assertTrue(bucket.conditionalPut(key, updated, predicate));
        assertEquals(updated, bucket.get(key));
    }

    @Test
    public void testConditionalPutFailsBecauseUnsatisfied() throws StoreOperationException {
        final Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());
        Value updated = new Value(JSON_UPDATED.getBytes());

        bucket.put(key, value);

        Predicate predicate = new Predicate("test:unsatisfied");
        Condition condition = new Condition() {

            @Override
            public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
                if (value.get("test").equals(expression)) {
                    return true;
                } else {
                    return false;
                }
            }

        };

        bucket.setConditions(Maps.hash(new String[]{"test"}, new Condition[]{condition}));
        assertFalse(bucket.conditionalPut(key, updated, predicate));
    }

    @Test
    public void testPutAndConditionallyGetValue() throws StoreOperationException {
        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());
        Predicate predicate = new Predicate("test:test");
        Condition condition = new Condition() {

            @Override
            public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
                return value.get(expression) != null;
            }

        };

        bucket.setConditions(Maps.hash(new String[]{"test"}, new Condition[]{condition}));
        bucket.put(key, value);
        assertEquals(value, bucket.conditionalGet(key, predicate));
    }

    @Test
    public void testPutAndConditionallyGetValues() throws StoreOperationException {
        Key key1 = new Key("key1");
        Key key2 = new Key("key2");
        Value value1 = new Value(JSON_VALUE.getBytes());
        Value value2 = new Value(JSON_VALUE_2.getBytes());
        Predicate predicate = new Predicate("test:test");
        Condition condition = new Condition() {

            @Override
            public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
                return value.get(expression) != null;
            }

        };

        bucket.setConditions(Maps.hash(new String[]{"test"}, new Condition[]{condition}));
        bucket.put(key1, value1);
        bucket.put(key2, value2);
        Map<Key, Value> result = bucket.conditionalGet(Sets.hash(key1, key2), predicate);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(key1));
    }

    @Test
    public void testPutAndConditionallyGetValueOnKey() throws StoreOperationException {
        final Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());
        Predicate predicate = new Predicate("test:test");
        Condition condition = new Condition() {

            @Override
            public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
                if (key.equals("key")) {
                    return true;
                } else {
                    return false;
                }
            }

        };


        bucket.setConditions(Maps.hash(new String[]{"test"}, new Condition[]{condition}));
        bucket.put(key, value);
        assertEquals(value, bucket.conditionalGet(key, predicate));
    }

    @Test
    public void testPutAndConditionallyGetValueNotFound() throws StoreOperationException {
        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());
        Predicate predicate = new Predicate("test:notfound");
        Condition condition = new Condition() {

            @Override
            public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
                return value.get(expression) != null;
            }

        };


        bucket.setConditions(Maps.hash(new String[]{"test"}, new Condition[]{condition}));
        bucket.put(key, value);
        assertNull(bucket.conditionalGet(key, predicate));
    }
    
    @Test
    public void testPutAndConditionallyRemoveValueWhenConditionIsSatisfied() throws StoreOperationException {
        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());
        Predicate predicate = new Predicate("test:test");
        Condition condition = new Condition() {
            @Override
            public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
                return value.get(expression) != null;
            }
        };
        
        bucket.setConditions(Maps.hash(new String[]{"test"}, new Condition[]{condition}));
        bucket.put(key, value);
        assertTrue(bucket.conditionalRemove(key, predicate));
        try {
            bucket.get(key);
            fail("Key should not have been found.");
        } catch (StoreOperationException e) {
            
        }
    }
    
    @Test
    public void testPutAndConditionallyRemoveValueWhenConditionIsUnsatisfied() throws StoreOperationException {
        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());
        Predicate predicate = new Predicate("test:notfound");
        Condition condition = new Condition() {

            @Override
            public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
                return value.get(expression) != null;
            }
        };
        
        bucket.setConditions(Maps.hash(new String[]{"test"}, new Condition[]{condition}));
        bucket.put(key, value);
        assertFalse(bucket.conditionalRemove(key, predicate));        
        assertNotNull(bucket.get(key));
    }
    
    @Test
    public void testConditionallyRemoveValueWhenValueDoesNotExist() throws StoreOperationException { 
        Key key = new Key("key");
        Predicate predicate = new Predicate("test:test");
        Condition condition = new Condition() {
            @Override
            public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
                return value.get(expression) != null;
            }
        };
        
        bucket.setConditions(Maps.hash(new String[]{"test"}, new Condition[]{condition}));
        assertFalse(bucket.conditionalRemove(key, predicate));
    }

    @Test(expected = StoreOperationException.class)
    public void testGetNullValue() throws StoreOperationException {
        Key key = new Key("key");
        bucket.get(key);
    }

    @Test
    public void testRemoveIsIdempotent() throws StoreOperationException {
        EventBus bus = createMock(EventBus.class);

        replay(bus);

        Key key = new Key("key");
        bucket.setEventBus(bus);
        bucket.remove(key);

        verify(bus);
    }

    @Test(expected = StoreOperationException.class)
    public void testPutAndRemoveValue() throws StoreOperationException {
        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());
        bucket.put(key, value);
        bucket.remove(key);
        bucket.get(key);
    }
    
    @Test
    public void testKeys() throws StoreOperationException {
        Key key1 = new Key("key");
        Value value1 = new Value(JSON_VALUE.getBytes());
        Key key2 = new Key("key2");
        Value value2 = new Value(JSON_VALUE.getBytes());
        bucket.put(key1, value1);
        bucket.put(key2, value2);
        assertEquals(2, bucket.keys().size());
        assertTrue(bucket.keys().contains(key1));
        assertTrue(bucket.keys().contains(key2));
    }

    @Test
    public void testKeysInRange() throws StoreOperationException {
        Comparator stringComparator = new Comparator() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }

        };

        Key key1 = new Key("key1");
        Value value1 = new Value(JSON_VALUE.getBytes());
        Key key2 = new Key("key2");
        Value value2 = new Value(JSON_VALUE.getBytes());
        Key key3 = new Key("key3");
        Value value3 = new Value(JSON_VALUE.getBytes());

        bucket.setComparators(Maps.hash(new String[]{"order"}, new Comparator[]{stringComparator}));
        bucket.put(key1, value1);
        bucket.put(key2, value2);
        bucket.put(key3, value3);
        Set<Key> range = bucket.keysInRange(new Range(new Key("key2"), new Key("key3"), 0, "order", 0));
        assertEquals(2, range.size());
        assertEquals(key2, range.toArray()[0]);
        assertEquals(key3, range.toArray()[1]);
    }

    @Test
    public void testKeysInRangeWithLimit() throws StoreOperationException {
        Comparator stringComparator = new Comparator() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }

        };

        bucket.setComparators(Maps.hash(new String[]{"order"}, new Comparator[]{stringComparator}));

        Key key1 = new Key("key1");
        Value value1 = new Value(JSON_VALUE.getBytes());
        Key key2 = new Key("key2");
        Value value2 = new Value(JSON_VALUE.getBytes());
        Key key3 = new Key("key3");
        Value value3 = new Value(JSON_VALUE.getBytes());
        bucket.put(key1, value1);
        bucket.put(key2, value2);
        bucket.put(key3, value3);
        Set<Key> range = bucket.keysInRange(new Range(new Key("key1"), new Key("key3"), 2, "order", 0));
        assertEquals(2, range.size());
        assertEquals(key1, range.toArray()[0]);
        assertEquals(key2, range.toArray()[1]);
    }

    @Test
    public void testKeysOutOfRange() throws StoreOperationException {
        Comparator stringComparator = new Comparator() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }

        };

        bucket.setComparators(Maps.hash(new String[]{"order"}, new Comparator[]{stringComparator}));

        Key key1 = new Key("key1");
        Value value1 = new Value(JSON_VALUE.getBytes());
        bucket.put(key1, value1);
        assertEquals(0, bucket.keysInRange(new Range(new Key("key2"), new Key("key3"), 0, "order", 0)).size());
    }

    @Test
    public void testKeysOutOfRightRange() throws StoreOperationException {
        Comparator stringComparator = new Comparator() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }

        };

        bucket.setComparators(Maps.hash(new String[]{"order"}, new Comparator[]{stringComparator}));

        Key key1 = new Key("key1");
        Value value1 = new Value(JSON_VALUE.getBytes());
        bucket.put(key1, value1);
        assertEquals(1, bucket.keysInRange(new Range(new Key("key1"), new Key("key2"), 0, "order", 0)).size());
    }

    @Test
    public void testKeysOutOfLeftRange() throws StoreOperationException {
        Comparator stringComparator = new Comparator() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }

        };

        bucket.setComparators(Maps.hash(new String[]{"order"}, new Comparator[]{stringComparator}));

        Key key1 = new Key("key1");
        Value value1 = new Value(JSON_VALUE.getBytes());
        bucket.put(key1, value1);
        assertEquals(1, bucket.keysInRange(new Range(new Key("key0"), new Key("key1"), 0, "order", 0)).size());
    }

    @Test
    public void testKeysInExactRange() throws StoreOperationException {
        Comparator stringComparator = new Comparator() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }

        };

        bucket.setComparators(Maps.hash(new String[]{"order"}, new Comparator[]{stringComparator}));

        Key key1 = new Key("key1");
        Value value1 = new Value(JSON_VALUE.getBytes());
        bucket.put(key1, value1);
        assertEquals(1, bucket.keysInRange(new Range(new Key("key1"), new Key("key1"), 0, "order", 0)).size());
    }

    @Test
    public void testUpdate() throws StoreOperationException, UnsupportedEncodingException {
        long timeoutInMillis = 1000;
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("p1", "value1");
        Update update = new Update("function", timeoutInMillis, params);
        Function function = new Function() {

            @Override
            public Map<String, Object> apply(String key, Map<String, Object> value, Map<String, Object> parameters) {
                value.put("test", parameters.get("p1"));
                return value;
            }

        };

        bucket.setFunctions(Maps.hash(new String[]{"function"}, new Function[]{function}));

        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes("UTF-8"));
        bucket.put(key, value);
        Value updated = bucket.update(key, update);
        assertArrayEquals(JSON_UPDATED.getBytes("UTF-8"), updated.getBytes());
        assertArrayEquals(JSON_UPDATED.getBytes("UTF-8"), bucket.get(key).getBytes());
    }

    @Test
    public void testMap() throws Exception {
        Mapper mapper = new Mapper("mapper", "combiner", 60000, Collections.EMPTY_MAP);
        Map<String, Object> mapResult = Maps.hash(new String[]{"m"}, new Object[]{"r"});

        Function mapFunction = createMock(Function.class);
        mapFunction.apply(eq("key"), eq(Maps.hash(new String[]{"test"}, new Object[]{"test"})), eq(Collections.EMPTY_MAP));
        expectLastCall().andReturn(mapResult).once();

        replay(mapFunction);

        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes("UTF-8"));
        bucket.setFunctions(Maps.hash(new String[]{"mapper"}, new Function[]{mapFunction}));
        bucket.put(key, value);
        assertEquals(mapResult, bucket.map(key, mapper));

        verify(mapFunction);
    }

    @Test
    public void testMapToNotExistentKey() throws Exception {
        Mapper mapper = new Mapper("mapper", "combiner", 60000, Collections.EMPTY_MAP);

        Function mapFunction = createMock(Function.class);

        replay(mapFunction);

        Key key = new Key("key");
        bucket.setFunctions(Maps.hash(new String[]{"mapper"}, new Function[]{mapFunction}));
        assertNull(bucket.map(key, mapper));

        verify(mapFunction);
    }

    @Test
    public void testBackupImportDoesNotDeleteExistentData() throws StoreOperationException {
        System.setProperty(Constants.TERRASTORE_HOME, System.getProperty("java.io.tmpdir"));
        new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + Constants.BACKUPS_DIR).mkdir();

        Key key1 = new Key("key1");
        Value value1 = new Value(JSON_VALUE.getBytes());
        Key key2 = new Key("key2");
        Value value2 = new Value(JSON_VALUE.getBytes());
        Key key3 = new Key("key3");
        Value value3 = new Value(JSON_VALUE.getBytes());

        bucket.put(key1, value1);
        bucket.put(key2, value2);

        assertTrue(bucket.keys().contains(key1));
        assertTrue(bucket.keys().contains(key2));
        bucket.exportBackup("test.bak");
        bucket.put(key3, value3);
        assertTrue(bucket.keys().contains(key3));

        bucket.importBackup("test.bak");
        assertTrue(bucket.keys().contains(key1));
        assertTrue(bucket.keys().contains(key2));
        assertTrue(bucket.keys().contains(key3));
    }

    private static class DisabledEventBus implements EventBus {

        @Override
        public ActionExecutor getActionExecutor() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List<EventListener> getEventListeners() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void publish(Event event) {
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isEnabled() {
            return false;
        }

    }
}
