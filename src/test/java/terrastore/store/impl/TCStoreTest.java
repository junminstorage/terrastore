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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import terrastore.internal.tc.TCMaster;
import terrastore.store.Bucket;
import terrastore.store.StoreOperationException;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class TCStoreTest {

    private TCStore store;

    @Before
    public void setUp() {
        TCMaster.getInstance().connect("localhost:9510", 1, TimeUnit.SECONDS);
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
}
