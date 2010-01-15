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

import java.util.Collection;
import org.terracotta.collections.ConcurrentDistributedMap;
import org.terracotta.collections.HashcodeLockStrategy;
import org.terracotta.collections.LockType;
import org.terracotta.modules.annotations.InstrumentedClass;
import terrastore.common.ErrorMessage;
import terrastore.store.Bucket;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;

/**
 * @author Sergio Bossa
 */
@InstrumentedClass
public class TCStore implements Store {

    private final ConcurrentDistributedMap<String, Bucket> buckets;

    public TCStore() {
        buckets = new ConcurrentDistributedMap<String, Bucket>(LockType.WRITE, new HashcodeLockStrategy(false, true));
    }

    public void add(String bucket) throws StoreOperationException {
        Bucket existent = buckets.putIfAbsent(bucket, new TCBucket(bucket));
        if (existent != null) {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.FORBIDDEN_ERROR_CODE, "Bucket already existent: " + bucket));
        }
    }

    public void remove(String bucket) throws StoreOperationException {
        Bucket removed = buckets.remove(bucket);
        if (removed == null) {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE, "Bucket not found: " + bucket));
        }
    }

    public Bucket get(String bucket) throws StoreOperationException {
        Bucket requested = buckets.get(bucket);
        if (requested != null) {
            return requested;
        } else {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE, "Bucket not found: " + bucket));
        }
    }

    @Override
    public Collection<Bucket> buckets() {
        return buckets.values();
    }
}
