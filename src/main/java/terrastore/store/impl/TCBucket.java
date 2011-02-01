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
import terrastore.store.LockManager;
import terrastore.store.SnapshotManager;
import terrastore.store.SortedSnapshot;
import terrastore.store.StoreOperationException;
import terrastore.store.ValidationException;
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
import terrastore.util.concurrent.GlobalExecutor;

/**
 * @author Sergio Bossa
 */
public class TCBucket implements Bucket {

    private static final Logger LOG = LoggerFactory.getLogger(TCBucket.class);
    //
    private final String name;
    private final ClusteredMap<String, byte[]> bucket;
    private boolean compressedDocuments;
    private EventBus eventBus;
    private SnapshotManager snapshotManager;
    private BackupManager backupManager;
    private LockManager lockManager;
    private Comparator defaultComparator = new LexicographicalComparator(true);
    private final Map<String, Comparator> comparators = new HashMap<String, Comparator>();
    private final Map<String, Condition> conditions = new HashMap<String, Condition>();
    private final Map<String, Function> updaters = new HashMap<String, Function>();
    private final Map<String, Function> mappers = new HashMap<String, Function>();

    public TCBucket(String name) {
        this.name = name;
        this.bucket = TCMaster.getInstance().getUnlockedMap(TCBucket.class.getName() + ".bucket." + name);
    }

    public String getName() {
        return name;
    }

    public void put(Key key, Value value) {
        // Use explicit locking to put and publish on the same "transactional" boundary and keep ordering under concurrency.
        lockWrite(key);
        try {
            Value old = doGet(key);
            doPut(key, value);
            if (eventBus.isEnabled()) {
                eventBus.publish(new ValueChangedEvent(name, key.toString(), old, value));
            }
        } finally {
            unlockWrite(key);
        }
    }

    public boolean conditionalPut(Key key, Value value, Predicate predicate) throws StoreOperationException {
        // Use explicit locking to put and publish on the same "transactional" boundary and keep ordering under concurrency.
        lockWrite(key);
        try {
            Condition condition = getCondition(predicate.getConditionType());
            Value old = doGet(key);
            if (old == null || old.dispatch(key, predicate, condition)) {
                doPut(key, value);
                if (eventBus.isEnabled()) {
                    eventBus.publish(new ValueChangedEvent(name, key.toString(), old, value));
                }
                return true;
            } else {
                return false;
            }
        } finally {
            unlockWrite(key);
        }
    }

    public boolean conditionalRemove(Key key, Predicate predicate) throws StoreOperationException {
        // Use explicit locking to make sure we see a consistent state while examining, removing and publishing.
        lockWrite(key);
        try {
            Condition condition = getCondition(predicate.getConditionType());
            Value value = doGet(key);
            if (value != null && value.dispatch(key, predicate, condition)) {
                doRemove(key);
                if (eventBus.isEnabled()) {
                    eventBus.publish(new ValueRemovedEvent(name, key.toString(), value));
                }
                return true;
            } else {
                return false;
            }
        } finally {
            unlockWrite(key);
        }
    }

    public Value get(Key key) throws StoreOperationException {
        Value value = doGet(key);
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
            Value value = doGet(key);
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    @Override
    public Value conditionalGet(Key key, Predicate predicate) throws StoreOperationException {
        Value value = doGet(key);
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
            Value value = doGet(key);
            Condition condition = getCondition(predicate.getConditionType());
            if (value.dispatch(key, predicate, condition)) {
                result.put(key, value);
            }
        }
        return result;
    }

    public void remove(Key key) throws StoreOperationException {
        // Use explicit locking to remove and publish on the same "transactional" boundary and keep ordering under concurrency.
        lockWrite(key);
        try {
            Value removed = doGet(key);
            doRemove(key);
            if (removed != null) {
                if (eventBus.isEnabled()) {
                    eventBus.publish(new ValueRemovedEvent(name, key.toString(), removed));
                }
            }
        } finally {
            unlockWrite(key);
        }
    }

    @Override
    public Value update(final Key key, final Update update) throws StoreOperationException {
        long timeout = update.getTimeoutInMillis();
        Future<Value> task = null;
        // Use explicit locking to update and block concurrent operations on the same key,
        // and also publish on the same "transactional" boundary and keep ordering under concurrency.
        lockWrite(key);
        try {
            final Value value = doGet(key);
            if (value != null) {
                final Function function = getFunction(updaters, update.getFunctionName());
                task = GlobalExecutor.getUpdateExecutor().submit(new Callable<Value>() {

                    @Override
                    public Value call() {
                        return value.dispatch(key, update, function);
                    }

                });
                Value result = task.get(timeout, TimeUnit.MILLISECONDS);
                doPut(key, result);
                if (eventBus.isEnabled()) {
                    eventBus.publish(new ValueChangedEvent(name, key.toString(), value, result));
                }
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
            unlockWrite(key);
        }
    }

    @Override
    public Value merge(Key key, Value value) throws StoreOperationException {
        // Use explicit locking to update and block concurrent operations on the same key,
        // and also publish on the same "transactional" boundary and keep ordering under concurrency.
        lockWrite(key);
        try {
            Value old = doGet(key);
            Value result = null;
            if (old != null) {
                result = old.merge(value);
                doPut(key, result);
                if (eventBus.isEnabled()) {
                    eventBus.publish(new ValueChangedEvent(name, key.toString(), old, result));
                }
                return result;
            } else {
                throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE, "Key not found: " + key));
            }
        } catch (StoreOperationException ex) {
            throw ex;
        } catch (ValidationException ex) {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, ex.getMessage()));
        } finally {
            unlockWrite(key);
        }
    }

    public Map<String, Object> map(final Key key, final Mapper mapper) throws StoreOperationException {
        Value value = doGet(key);
        if (value != null) {
            Function function = getFunction(mappers, mapper.getMapperName());
            return value.dispatch(key, mapper, function);
        } else {
            return null;
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
            LOG.warn("Request to flush {} keys on bucket {}", keys.size(), name);
            flushStrategy.flush(this, keys, flushCondition, new FlushCallback() {

                @Override
                public void doFlush(Key key) {
                    Value value = doGet(key);
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
    public void setCompressDocuments(boolean compressed) {
        this.compressedDocuments = compressed;
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
    public void setLockManager(LockManager lockManager) {
        this.lockManager = lockManager;
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
    public void setUpdaters(Map<String, Function> functions) {
        this.updaters.clear();
        this.updaters.putAll(functions);
    }

    @Override
    public void setMappers(Map<String, Function> functions) {
        this.mappers.clear();
        this.mappers.putAll(functions);
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
        } else {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Wrong comparator name: " + comparatorName));
        }
    }

    private Function getFunction(Map<String, Function> functions, String functionName) throws StoreOperationException {
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

    private void lockRead(Key key) {
        lockManager.lockRead(name, key);
    }

    private void unlockRead(Key key) {
        lockManager.unlockRead(name, key);
    }

    private void lockWrite(Key key) {
        lockManager.lockWrite(name, key);
    }

    private void unlockWrite(Key key) {
        lockManager.unlockWrite(name, key);
    }

    private byte[] valueToBytes(Value value) {
        if (value != null) {
            return compressedDocuments ? value.getCompressedBytes() : value.getBytes();
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

    private Value doGet(Key key) {
        Value value = bytesToValue(bucket.unsafeGet(key.toString()));
        if (value == null) {
            lockRead(key);
            try {
                value = bytesToValue(bucket.unlockedGet(key.toString()));
            } finally {
                unlockRead(key);
            }
        }
        return value;
    }

    private void doRemove(Key key) {
        bucket.unlockedRemoveNoReturn(key.toString());
    }

    private void doPut(Key key, Value value) {
        bucket.unlockedPutNoReturn(key.toString(), valueToBytes(value));
    }

    private static class KeyDeserializer implements Transformer<String, Key> {

        @Override
        public Key transform(String input) {
            return new Key(input);
        }

    }
}
