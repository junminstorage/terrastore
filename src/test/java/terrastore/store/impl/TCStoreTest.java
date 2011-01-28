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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import terrastore.internal.tc.TCMaster;
import terrastore.store.Bucket;
import terrastore.store.Key;
import terrastore.store.StoreOperationException;
import terrastore.store.features.Mapper;
import terrastore.store.features.Reducer;
import terrastore.store.operators.Aggregator;
import terrastore.util.collect.Maps;
import terrastore.util.collect.Sets;
import terrastore.util.json.JsonUtils;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class TCStoreTest {

    private TCStore store;

    @Before
    public void setUp() {
        TCMaster.getInstance().setupLocally();
        store = new TCStore();
    }

    @Test
    public void testGetOrCreate() throws StoreOperationException {
        String bucket = "bucket";
        Bucket created = store.getOrCreate(bucket);
        assertNotNull(created);
        Bucket got = store.getOrCreate(bucket);
        assertNotNull(got);
        assertSame(created, got);
    }

    @Test
    public void testGetOrCreateThenGet() throws StoreOperationException {
        String bucket = "bucket";
        Bucket created = store.getOrCreate(bucket);
        assertNotNull(created);
        Bucket got = store.get(bucket);
        assertNotNull(got);
        assertSame(created, got);
    }

    @Test
    public void testGetNullBucket() throws StoreOperationException {
        String bucket = "bucket";
        Bucket notExistent = store.get(bucket);
        assertNull(notExistent);
    }

    @Test
    public void testGetOrCreateOnMultiThread() throws StoreOperationException {
        final String bucket = "bucket";
        final AtomicReference<Bucket> bucketRef = new AtomicReference<Bucket>();
        final AtomicBoolean failed = new AtomicBoolean(false);
        for (int i = 0; i < 1000; i++) {
            ExecutorService executor = Executors.newCachedThreadPool();
            executor.submit(new Runnable() {

                @Override
                public void run() {
                    if (failed.get()) {
                        Bucket current = store.getOrCreate(bucket);
                        Bucket old = bucketRef.getAndSet(current);
                        if (old != null && old != current) {
                            failed.set(true);
                        }
                    }
                }
            });
        }
        assertFalse(failed.get());
    }

    @Test
    public void testGetAllBuckets() throws StoreOperationException {
        String bucket1 = "bucket1";
        String bucket2 = "bucket2";
        store.getOrCreate(bucket1);
        store.getOrCreate(bucket2);
        assertEquals(2, store.buckets().size());
    }

    @Test
    public void testRemoveBucket() throws StoreOperationException {
        String bucket = "bucket";
        Bucket created1 = store.getOrCreate(bucket);
        store.remove(bucket);
        Bucket created2 = store.getOrCreate(bucket);
        assertNotSame(created1, created2);
    }

    @Test
    public void testRemoveSameBucketTwiceIsIdempotent() {
        String bucket = "bucket";
        store.getOrCreate(bucket);
        store.remove(bucket);
        store.remove(bucket);
    }

    @Test
    public void testRemoveNotExistentBucketHasNoEffect() throws StoreOperationException {
        String bucket = "bucket";
        store.remove(bucket);
    }

    @Test
    public void testMap() throws Exception {
        String bucketName = "bucket";
        Set<Key> keys = Sets.hash(new Key("k1"), new Key("k2"));
        Mapper mapper = new Mapper("mapper", "combiner", 60000, Collections.EMPTY_MAP);
        Map<String, Object> mapResult1 = Maps.hash(new String[]{"k1"}, new Object[]{"v1"});
        Map<String, Object> mapResult2 = Maps.hash(new String[]{"k2"}, new Object[]{"v2"});
        Map<String, Object> combinerResult = Maps.hash(new String[]{"c1"}, new Object[]{"c2"});

        Bucket bucket = createMock(Bucket.class);
        makeThreadSafe(bucket, true);
        bucket.map(eq(new Key("k1")), same(mapper));
        expectLastCall().andReturn(mapResult1).once();
        bucket.map(eq(new Key("k2")), same(mapper));
        expectLastCall().andReturn(mapResult2).once();
        TCStore mockedStore = createMockBuilder(TCStore.class).addMockedMethod(TCStore.class.getDeclaredMethod("get", String.class)).withConstructor().createMock();
        mockedStore.get(bucketName);
        expectLastCall().andReturn(bucket).once();
        Aggregator aggregator = createMock(Aggregator.class);
        Capture<List<Map<String, Object>>> combinerCapture = new Capture<List<Map<String, Object>>>();
        makeThreadSafe(aggregator, true);
        aggregator.apply(EasyMock.capture(combinerCapture), EasyMock.eq(Collections.EMPTY_MAP));
        expectLastCall().andReturn(combinerResult).once();

        replay(mockedStore, bucket, aggregator);

        mockedStore.setCombiners(Maps.hash(new String[]{"combiner"}, new Aggregator[]{aggregator}));
        assertEquals(combinerResult, mockedStore.map(bucketName, keys, mapper));
        assertTrue(combinerCapture.getValue().equals(Arrays.asList(mapResult1, mapResult2)) || combinerCapture.getValue().equals(Arrays.asList(mapResult2, mapResult1)));

        verify(mockedStore, bucket, aggregator);
    }

    @Test
    public void testMapToNonExistentKey() throws Exception {
        String bucketName = "bucket";
        Set<Key> keys = Sets.hash(new Key("k1"), new Key("k2"));
        Mapper mapper = new Mapper("mapper", "combiner", 60000, Collections.EMPTY_MAP);
        Map<String, Object> mapResult1 = Maps.hash(new String[]{"k1"}, new Object[]{"v1"});
        Map<String, Object> mapResult2 = null;
        Map<String, Object> combinerResult = Maps.hash(new String[]{"c1"}, new Object[]{"c2"});

        Bucket bucket = createMock(Bucket.class);
        makeThreadSafe(bucket, true);
        bucket.map(eq(new Key("k1")), same(mapper));
        expectLastCall().andReturn(mapResult1).once();
        bucket.map(eq(new Key("k2")), same(mapper));
        expectLastCall().andReturn(mapResult2).once();
        TCStore mockedStore = createMockBuilder(TCStore.class).addMockedMethod(TCStore.class.getDeclaredMethod("get", String.class)).withConstructor().createMock();
        mockedStore.get(bucketName);
        expectLastCall().andReturn(bucket).once();
        Aggregator aggregator = createMock(Aggregator.class);
        Capture<List<Map<String, Object>>> combinerCapture = new Capture<List<Map<String, Object>>>();
        makeThreadSafe(aggregator, true);
        aggregator.apply(EasyMock.capture(combinerCapture), EasyMock.eq(Collections.EMPTY_MAP));
        expectLastCall().andReturn(combinerResult).once();

        replay(mockedStore, bucket, aggregator);

        mockedStore.setCombiners(Maps.hash(new String[]{"combiner"}, new Aggregator[]{aggregator}));
        assertEquals(combinerResult, mockedStore.map(bucketName, keys, mapper));
        assertEquals(Arrays.asList(mapResult1), combinerCapture.getValue());

        verify(mockedStore, bucket, aggregator);
    }

    @Test
    public void testReduce() throws Exception {
        Map<String, Object> mapResult1 = Maps.hash(new String[]{"k1"}, new Object[]{"v1"});
        Map<String, Object> mapResult2 = Maps.hash(new String[]{"k2"}, new Object[]{"v2"});
        List<Map<String, Object>> allResults = Arrays.asList(mapResult1, mapResult2);
        Reducer reducer = new Reducer("reducer", 60000, Collections.EMPTY_MAP);
        Map<String, Object> reducerResult = Maps.hash(new String[]{"r1"}, new Object[]{"r2"});

        TCStore mockedStore = createMockBuilder(TCStore.class).withConstructor().createMock();
        Aggregator aggregator = createMock(Aggregator.class);
        makeThreadSafe(aggregator, true);
        aggregator.apply(same(allResults), eq(Collections.EMPTY_MAP));
        expectLastCall().andReturn(reducerResult).once();

        replay(mockedStore, aggregator);

        mockedStore.setReducers(Maps.hash(new String[]{"reducer"}, new Aggregator[]{aggregator}));
        assertEquals(JsonUtils.fromMap(reducerResult), mockedStore.reduce(allResults, reducer));

        verify(mockedStore, aggregator);
    }
}
