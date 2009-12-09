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
package terrastore.store.impl;

import java.io.UnsupportedEncodingException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.Before;
import org.junit.Test;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.operators.Function;
import terrastore.store.features.Update;
import terrastore.store.features.Range;
import terrastore.store.operators.Condition;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class TCBucketTest {

    private static final String JSON_VALUE = "{\"test\":\"test\"}";
    private static final String JSON_UPDATED = "{\"test\":\"value1\"}";
    private TCBucket bucket;

    @Before
    public void setUp() {
        bucket = new TCBucket("bucket");
    }

    @Test
    public void testPutAndGetValue() throws StoreOperationException {
        String key = "key";
        Value value = new Value(JSON_VALUE.getBytes());
        bucket.put(key, value);
        assertEquals(value, bucket.get(key));
    }

    @Test
    public void testPutAndConditionallyGetValue() throws StoreOperationException {
        String key = "key";
        Value value = new Value(JSON_VALUE.getBytes());
        Predicate predicate = new Predicate("test:test");
        Condition condition = new Condition() {

            @Override
            public boolean isSatisfied(Map<String, Object> value, String expression) {
                return value.get(expression) != null;
            }
        };

        bucket.put(key, value);
        assertEquals(value, bucket.conditionalGet(key, predicate, condition));
    }

    @Test
    public void testPutAndConditionallyGetValue2() throws StoreOperationException {
        String key = "key";
        Value value = new Value(JSON_VALUE.getBytes());
        Predicate predicate = new Predicate("test:notfound");
        Condition condition = new Condition() {

            @Override
            public boolean isSatisfied(Map<String, Object> value, String expression) {
                return value.get(expression) != null;
            }
        };

        bucket.put(key, value);
        assertEquals(null, bucket.conditionalGet(key, predicate, condition));
    }

    @Test(expected = StoreOperationException.class)
    public void testGetNullValue() throws StoreOperationException {
        String key = "key";
        bucket.get(key);
    }

    @Test(expected = StoreOperationException.class)
    public void testRemoveNullValue() throws StoreOperationException {
        String key = "key";
        bucket.remove(key);
    }

    @Test(expected = StoreOperationException.class)
    public void testPutAndRemoveValue() throws StoreOperationException {
        String key = "key";
        Value value = new Value(JSON_VALUE.getBytes());
        bucket.put(key, value);
        bucket.remove(key);
        bucket.get(key);
    }

    @Test
    public void testKeys() throws StoreOperationException {
        String key1 = "key1";
        Value value1 = new Value(JSON_VALUE.getBytes());
        String key2 = "key2";
        Value value2 = new Value(JSON_VALUE.getBytes());
        bucket.put(key1, value1);
        bucket.put(key2, value2);
        assertEquals(2, bucket.keys().size());
        assertTrue(bucket.keys().contains(key1));
        assertTrue(bucket.keys().contains(key2));
    }

    @Test
    public void testKeysInRange() throws StoreOperationException {
        Comparator<String> stringComparator = new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };

        String key1 = "key1";
        Value value1 = new Value(JSON_VALUE.getBytes());
        String key2 = "key2";
        Value value2 = new Value(JSON_VALUE.getBytes());
        String key3 = "key3";
        Value value3 = new Value(JSON_VALUE.getBytes());
        bucket.put(key1, value1);
        bucket.put(key2, value2);
        bucket.put(key3, value3);
        assertEquals(2, bucket.keysInRange(new Range("key2", "key3", "order"), stringComparator, 0).size());
        assertTrue(bucket.keys().contains(key2));
        assertTrue(bucket.keys().contains(key3));
    }

    @Test
    public void testKeysOutOfRange() throws StoreOperationException {
        Comparator<String> stringComparator = new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };

        String key1 = "key1";
        Value value1 = new Value(JSON_VALUE.getBytes());
        bucket.put(key1, value1);
        assertEquals(0, bucket.keysInRange(new Range("key2", "key3", "order"), stringComparator, 0).size());
    }

    @Test
    public void testKeysOutOfRightRange() throws StoreOperationException {
        Comparator<String> stringComparator = new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };

        String key1 = "key1";
        Value value1 = new Value(JSON_VALUE.getBytes());
        bucket.put(key1, value1);
        assertEquals(1, bucket.keysInRange(new Range("key1", "key2", "order"), stringComparator, 0).size());
    }

    @Test
    public void testKeysOutOfLeftRange() throws StoreOperationException {
        Comparator<String> stringComparator = new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };

        String key1 = "key1";
        Value value1 = new Value(JSON_VALUE.getBytes());
        bucket.put(key1, value1);
        assertEquals(1, bucket.keysInRange(new Range("key0", "key1", "order"), stringComparator, 0).size());
    }

    @Test
    public void testKeysInExactRange() throws StoreOperationException {
        Comparator<String> stringComparator = new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };

        String key1 = "key1";
        Value value1 = new Value(JSON_VALUE.getBytes());
        bucket.put(key1, value1);
        assertEquals(1, bucket.keysInRange(new Range("key1", "key1", "order"), stringComparator, 0).size());
    }

    @Test()
    public void testUpdate() throws StoreOperationException, UnsupportedEncodingException {
        long timeoutInMillis = 1000;
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("p1", "value1");
        Update update = new Update("function", timeoutInMillis, params);
        Function function = new Function() {

            @Override
            public Map<String, Object> apply(Map<String, Object> value, Map<String, Object> parameters) {
                value.put("test", parameters.get("p1"));
                return value;
            }
        };

        String key = "key";
        Value value = new Value(JSON_VALUE.getBytes("UTF-8"));
        bucket.put(key, value);
        bucket.update(key, update, function, Executors.newCachedThreadPool());
        assertEquals(new String(JSON_UPDATED.getBytes("UTF-8")), new String(bucket.get(key).getBytes()));
    }
}
