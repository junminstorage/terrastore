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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.cluster.ClusterInfo;
import org.terracotta.collections.ClusteredMap;
import org.terracotta.locking.ClusteredLock;
import terrastore.internal.tc.TCMaster;
import terrastore.common.ErrorMessage;
import terrastore.event.EventBus;
import terrastore.event.impl.ValueChangedEvent;
import terrastore.event.impl.ValueRemovedEvent;
import terrastore.store.comparators.LexicographicalComparator;
import terrastore.store.BackupManager;
import terrastore.store.Bucket;
import terrastore.store.FlushCallback;
import terrastore.store.FlushCondition;
import terrastore.store.FlushStrategy;
import terrastore.store.Key;
import terrastore.store.SnapshotManager;
import terrastore.store.SortedSnapshot;
import terrastore.store.StoreOperationException;
import terrastore.store.features.Predicate;
import terrastore.store.features.Update;
import terrastore.store.Value;
import terrastore.store.features.Mapper;
import terrastore.store.operators.Condition;
import terrastore.store.operators.Function;
import terrastore.store.features.Range;
import terrastore.store.operators.Comparator;
import terrastore.util.collect.Sets;
import terrastore.util.collect.Transformer;
import terrastore.util.global.GlobalExecutor;

/**
 * @author Sergio Bossa
 */
public class TCBucket implements Bucket {

    private static final Logger LOG = LoggerFactory.getLogger(TCBucket.class);
    //
    private final String name;
    private final ClusteredMap<String, byte[]> bucket;
    private EventBus eventBus;
    private SnapshotManager snapshotManager;
    private BackupManager backupManager;
    private Comparator defaultComparator = new LexicographicalComparator(true);
    private final Map<String, Comparator> comparators = new HashMap<String, Comparator>();
    private final Map<String, Function> functions = new HashMap<String, Function>();
    private final Map<String, Condition> conditions = new HashMap<String, Condition>();

    public TCBucket(String name) {
        this.name = name;
        this.bucket = TCMaster.getInstance().getMap(TCBucket.class.getName() + ".bucket." + name);
    }

    public String getName() {
        return name;
    }

    public void put(Key key, Value value) {
        // Use explicit locking to put and publish on the same "transactional" boundary and keep ordering under concurrency.
        lock(key);
        try {
            byte[] old = bucket.put(key.toString(), valueToBytes(value));
            eventBus.publish(new ValueChangedEvent(name, key.toString(), old, value.getBytes()));
        } finally {
            unlock(key);
        }
    }

    public boolean conditionalPut(Key key, Value value, Predicate predicate) throws StoreOperationException {
        // Use explicit locking to put and publish on the same "transactional" boundary and keep ordering under concurrency.
        lock(key);
        try {
            Condition condition = getCondition(predicate.getConditionType());
            byte[] old = bucket.get(key.toString());
            if (old == null || bytesToValue(old).dispatch(key, predicate, condition)) {
                bucket.put(key.toString(), valueToBytes(value));
                eventBus.publish(new ValueChangedEvent(name, key.toString(), old, value.getBytes()));
                return true;
            } else {
                return false;
            }
        } finally {
            unlock(key);
        }
    }

    public Value get(Key key) throws StoreOperationException {
        Value value = innerGet(key);
        if (value != null) {
            return value;
        } else {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE, "Key not found: " + key));
        }
    }

    @Override
    public Map<Key, Value> get(Set<Key> keys) throws StoreOperationException {
        Map<Key, Value> result = new HashMap<Key, Value>(keys.size());
        for (Key key : keys) {
            Value value = innerGet(key);
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    @Override
    public Value conditionalGet(Key key, Predicate predicate) throws StoreOperationException {
        Value value = innerGet(key);
        if (value != null) {
            Condition condition = getCondition(predicate.getConditionType());
            if (value.dispatch(key, predicate, condition)) {
                return value;
            } else {
                return null;
            }
        } else {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE, "Key not found: " + key));
        }
    }

    @Override
    public Map<Key, Value> conditionalGet(Set<Key> keys, Predicate predicate) throws StoreOperationException {
        Map<Key, Value> result = new HashMap<Key, Value>(keys.size());
        for (Key key : keys) {
            Value value = innerGet(key);
            Condition condition = getCondition(predicate.getConditionType());
            if (value.dispatch(key, predicate, condition)) {
                result.put(key, value);
            }
        }
        return result;
    }

    public void remove(Key key) throws StoreOperationException {
        // Use explicit locking to remove and publish on the same "transactional" boundary and keep ordering under concurrency.
        lock(key);
        try {
            Value removed = bytesToValue(bucket.remove(key.toString()));
            if (removed != null) {
                eventBus.publish(new ValueRemovedEvent(name, key.toString(), removed.getBytes()));
            }
        } finally {
            unlock(key);
        }
    }

    @Override
    public Value update(final Key key, final Update update) throws StoreOperationException {
        long timeout = update.getTimeoutInMillis();
        Future<Value> task = null;
        // Use explicit locking to update and block concurrent operations on the same key,
        // and also publish on the same "transactional" boundary and keep ordering under concurrency.
        lock(key);
        try {
            final byte[] value = bucket.get(key.toString());
            if (value != null) {
                final Function function = getFunction(update.getFunctionName());
                task = GlobalExecutor.getStoreExecutor().submit(new Callable<Value>() {

                    @Override
                    public Value call() {
                        return bytesToValue(value).dispatch(key, update, function);
                    }

                });
                Value result = task.get(timeout, TimeUnit.MILLISECONDS);
                bucket.unlockedPutNoReturn(key.toString(), valueToBytes(result));
                eventBus.publish(new ValueChangedEvent(name, key.toString(), value, result.getBytes()));
                return result;
            } else {
                throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE, "Key not found: " + key));
            }
        } catch (StoreOperationException ex) {
            throw ex;
        } catch (TimeoutException ex) {
            task.cancel(true);
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Update cancelled due to long execution time."));
        } catch (Exception ex) {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
        } finally {
            unlock(key);
        }
    }

    public Map<String, Object> map(final Key key, final Mapper mapper) throws StoreOperationException {
        Future<Map<String, Object>> task = null;
        try {
            final byte[] value = bucket.get(key.toString());
            if (value != null) {
                final Function function = getFunction(mapper.getMapperName());
                task = GlobalExecutor.getStoreExecutor().submit(new Callable<Map<String, Object>>() {

                    @Override
                    public Map<String, Object> call() {
                        return bytesToValue(value).dispatch(key, mapper, function);
                    }

                });
                return task.get(mapper.getTimeoutInMillis(), TimeUnit.MILLISECONDS);
            } else {
                return null;
            }
        } catch (TimeoutException ex) {
            task.cancel(true);
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Aggregation cancelled due to long execution time."));
        } catch (Exception ex) {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
        }
    }

    @Override
    public void clear() {
        bucket.clear();
    }

    @Override
    public long size() {
        return bucket.size();
    }

    @Override
    public Set<Key> keys() {
        return Sets.transformed(bucket.keySet(), new KeyDeserializer());
    }

    @Override
    public Set<Key> keysInRange(Range keyRange) throws StoreOperationException {
        Comparator keyComparator = getComparator(keyRange.getKeyComparatorName());
        SortedSnapshot snapshot = snapshotManager.getOrComputeSortedSnapshot(this, keyComparator, keyRange.getKeyComparatorName(), keyRange.getTimeToLive());
        return snapshot.keysInRange(keyRange.getStartKey(), keyRange.getEndKey(), keyRange.getLimit());
    }

    @Override
    public void flush(FlushStrategy flushStrategy, FlushCondition flushCondition) {
        ClusterInfo cluster = TCMaster.getInstance().getClusterInfo();
        if (cluster != null) {
            Collection<Key> keys = Sets.transformed(cluster.<String>getKeysForLocalValues(bucket), new KeyDeserializer());
            LOG.info("Request to flush {} keys on bucket {}", keys.size(), name);
            flushStrategy.flush(this, keys, flushCondition, new FlushCallback() {

                @Override
                public void doFlush(Key key) {
                    Value value = bytesToValue(bucket.get(key.toString()));
                    bucket.flush(key, value);
                }

            });
        } else {
            LOG.warn("Running outside of cluster, no keys to flush!");
        }
    }

    @Override
    public void exportBackup(String destination) throws StoreOperationException {
        backupManager.exportBackup(this, destination);
    }

    @Override
    public void importBackup(String source) throws StoreOperationException {
        backupManager.importBackup(this, source);
    }

    @Override
    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public void setSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    @Override
    public void setBackupManager(BackupManager backupManager) {
        this.backupManager = backupManager;
    }

    @Override
    public void setDefaultComparator(Comparator defaultComparator) {
        this.defaultComparator = defaultComparator;
    }

    @Override
    public void setComparators(Map<String, Comparator> comparators) {
        this.comparators.clear();
        this.comparators.putAll(comparators);
    }

    @Override
    public void setFunctions(Map<String, Function> functions) {
        this.functions.clear();
        this.functions.putAll(functions);
    }

    @Override
    public void setConditions(Map<String, Condition> conditions) {
        this.conditions.clear();
        this.conditions.putAll(conditions);
    }

    private Comparator getComparator(String comparatorName) throws StoreOperationException {
        if (comparators.containsKey(comparatorName)) {
            return comparators.get(comparatorName);
        } else if (StringUtils.isBlank(comparatorName)) {
            return defaultComparator;
        }
        
        throw new StoreOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Unknown comparator name: " + comparatorName));
    }

    private Function getFunction(String functionName) throws StoreOperationException {
        if (functions.containsKey(functionName)) {
            return functions.get(functionName);
        } else {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Wrong function name: " + functionName));
        }
    }

    private Condition getCondition(String conditionType) throws StoreOperationException {
        if (conditions.containsKey(conditionType)) {
            return conditions.get(conditionType);
        } else {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Wrong condition type: " + conditionType));
        }
    }

    private void lock(Key key) {
        ClusteredLock lock = bucket.createFinegrainedLock(key.toString());
        lock.lock();
    }

    private void unlock(Key key) {
        ClusteredLock lock = bucket.createFinegrainedLock(key.toString());
        lock.unlock();
    }

    private byte[] valueToBytes(Value value) {
        if (value != null) {
            return value.getBytes();
        } else {
            return null;
        }
    }

    private Value bytesToValue(byte[] bytes) {
        if (bytes != null) {
            return new Value(bytes);
        } else {
            return null;
        }
    }

    private Value innerGet(Key key) {
        Value value = bytesToValue(bucket.unlockedGet(key.toString()));
        value = value != null ? value : bytesToValue(bucket.get(key.toString()));
        return value;
    }

    private static class KeyDeserializer implements Transformer<String, Key> {

        @Override
        public Key transform(String input) {
            return new Key(input);
        }

    }
}
