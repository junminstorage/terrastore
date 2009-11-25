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

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.terracotta.collections.ConcurrentDistributedMap;
import org.terracotta.collections.FinegrainedLock;
import org.terracotta.modules.annotations.InstrumentedClass;
import terrastore.common.ErrorMessage;
import terrastore.store.Bucket;
import terrastore.store.StoreOperationException;
import terrastore.store.features.Update;
import terrastore.store.Value;
import terrastore.store.function.Function;
import terrastore.store.features.Range;
import terrastore.util.JsonUtils;

/**
 * @author Sergio Bossa
 */
@InstrumentedClass
public class TCBucket implements Bucket {

    private final String name;
    private final ConcurrentDistributedMap<String, Value> bucket;

    public TCBucket(String name) {
        this.name = name;
        this.bucket = new ConcurrentDistributedMap<String, Value>();
    }

    public String getName() {
        return name;
    }

    public void put(String key, Value value) {
        bucket.putNoReturn(key, value);
    }

    public Value get(String key) throws StoreOperationException {
        Value value = bucket.get(key);
        if (value != null) {
            return value;
        } else {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE, "Key not found: " + key));
        }
    }

    public void remove(String key) throws StoreOperationException {
        Value removed = bucket.remove(key);
        if (removed == null) {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE, "Key not found: " + key));
        }
    }

    @Override
    public void update(final String key, final Update update, final Function function, final ExecutorService updateExecutor) throws StoreOperationException {
        long timeout = update.getTimeoutInMillis();
        boolean locked = lock(key);
        if (locked) {
            Future<Map<String, Object>> task = null;
            try {
                final Value value = bucket.get(key);
                task = updateExecutor.submit(new Callable<Map<String, Object>>() {

                    @Override
                    public Map<String, Object> call() {
                        return update.update(JsonUtils.toMap(value), function);
                    }
                });
                Map<String, Object> result = task.get(timeout, TimeUnit.MILLISECONDS);
                bucket.put(key, JsonUtils.fromMap(result));
            } catch (Exception ex) {
                task.cancel(true);
            } finally {
                unlock(key);
                if (task.isCancelled()) {
                    throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Update cancelled due to long execution time."));
                }
            }
        } else {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE, "Key not found: " + key));
        }
    }

    public Set<String> keys() {
        return bucket.keySet();
    }

    public Set<String> keysInRange(Range keyRange, Comparator<String> keyComparator) {
        if (keyRange != null && keyComparator != null) {
            return Sets.filter(bucket.keySet(), new RangePredicate(keyRange, keyComparator));
        } else {
            throw new IllegalArgumentException("Range and comparator cannot be null!");
        }
    }

    private boolean lock(String key) {
        if (bucket.containsKey(key)) {
            FinegrainedLock lock = bucket.createFinegrainedLock(key);
            lock.lock();
            return true;
        } else {
            return false;
        }
    }

    private void unlock(String key) {
        FinegrainedLock lock = bucket.createFinegrainedLock(key);
        lock.unlock();
    }

    private static class RangePredicate implements Predicate<String> {

        private final Range range;
        private final Comparator<String> keyComparator;

        public RangePredicate(Range range, Comparator<String> keyComparator) {
            this.range = range;
            this.keyComparator = keyComparator;
        }

        public boolean apply(String key) {
            return range.isInRange(key, keyComparator);
        }
    }
}
