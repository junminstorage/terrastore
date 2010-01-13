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

import org.junit.Before;
import org.junit.Test;
import terrastore.store.StoreOperationException;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class TCStoreTest {

    private TCStore store;

    @Before
    public void setUp() {
        store = new TCStore();
    }

    @Test
    public void testAddAndGetBucket() throws StoreOperationException {
        String bucket = "bucket";
        store.add(bucket);
        assertNotNull(store.get(bucket));
    }

    @Test
    public void testGetAllBuckets() throws StoreOperationException {
        String bucket1 = "bucket1";
        String bucket2 = "bucket2";
        store.add(bucket1);
        store.add(bucket2);
        assertEquals(2, store.buckets().size());
    }

    @Test(expected=StoreOperationException.class)
    public void testGetNullBucket() throws StoreOperationException {
        String bucket = "bucket";
        store.get(bucket);
    }

    @Test(expected=StoreOperationException.class)
    public void testRemoveNullBucket() throws StoreOperationException {
        String bucket = "bucket";
        store.remove(bucket);
    }

    @Test(expected=StoreOperationException.class)
    public void testAddAndRemoveBucket() throws StoreOperationException {
        String bucket = "bucket";
        store.add(bucket);
        assertNotNull(store.get(bucket));
        store.remove(bucket);
        store.get(bucket);
    }

    @Test(expected=StoreOperationException.class)
    public void testAddSameBucketTwiceMustFail() throws StoreOperationException {
        String bucket = "bucket";
        store.add(bucket);
        store.add(bucket);
    }
}