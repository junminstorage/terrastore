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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.collections.ClusteredMap;
import terrastore.common.ErrorMessage;
import terrastore.internal.tc.TCMaster;
import terrastore.event.EventBus;
import terrastore.service.comparators.LexicographicalComparator;
import terrastore.store.BackupManager;
import terrastore.store.Bucket;
import terrastore.store.FlushCondition;
import terrastore.store.FlushStrategy;
import terrastore.store.Key;
import terrastore.store.SnapshotManager;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import terrastore.store.features.Mapper;
import terrastore.store.features.Reducer;
import terrastore.store.operators.Aggregator;
import terrastore.store.operators.Comparator;
import terrastore.store.operators.Condition;
import terrastore.store.operators.Function;
import terrastore.util.collect.parallel.MapCollector;
import terrastore.util.collect.parallel.MapTask;
import terrastore.util.collect.parallel.ParallelUtils;
import terrastore.util.global.GlobalExecutor;
import terrastore.util.json.JsonUtils;

/**
 * @author Sergio Bossa
 */
public class TCStore implements Store {

    private static final Logger LOG = LoggerFactory.getLogger(TCStore.class);
    //
    // TODO: buckets marked with tombstones aren't currently removed, only cleared.
    private final static String TOMBSTONE = TCStore.class.getName() + ".TOMBSTONE";
    //
    private final ClusteredMap<String, String> buckets;
    private final ConcurrentMap<String, Bucket> instances;
    private final Map<String, Comparator> comparators = new HashMap<String, Comparator>();
    private final Map<String, Condition> conditions = new HashMap<String, Condition>();
    private final Map<String, Function> functions = new HashMap<String, Function>();
    private final Map<String, Aggregator> aggregators = new HashMap<String, Aggregator>();
    private Comparator defaultComparator = new LexicographicalComparator(true);
    private SnapshotManager snapshotManager;
    private BackupManager backupManager;
    private EventBus eventBus;

    public TCStore() {
        buckets = TCMaster.getInstance().getMap(TCStore.class.getName() + ".buckets");
        instances = new ConcurrentHashMap<String, Bucket>();
    }

    @Override
    public Bucket getOrCreate(String bucket) {
        Bucket requested = instances.get(bucket);
        if (requested == null) {
            buckets.lockEntry(bucket);
            try {
                if (!instances.containsKey(bucket)) {
                    Bucket created = new TCBucket(bucket);
                    hydrateBucket(created);
                    instances.put(bucket, created);
                    if (!buckets.containsKey(bucket) || buckets.get(bucket).equals(TOMBSTONE)) {
                        buckets.putNoReturn(bucket, bucket);
                    }
                    requested = created;
                } else {
                    requested = instances.get(bucket);
                }
            } finally {
                buckets.unlockEntry(bucket);
            }
        }
        return requested;
    }

    @Override
    public Bucket get(String bucket) {
        Bucket requested = instances.get(bucket);
        if (requested == null) {
            buckets.lockEntry(bucket);
            try {
                if (!instances.containsKey(bucket)) {
                    if (buckets.containsKey(bucket)) {
                        Bucket created = new TCBucket(bucket);
                        hydrateBucket(created);
                        instances.put(bucket, created);
                        requested = created;
                    }
                } else {
                    requested = instances.get(bucket);
                }
            } finally {
                buckets.unlockEntry(bucket);
            }
        }
        return requested;
    }

    @Override
    public void remove(String bucket) {
        buckets.lockEntry(bucket);
        try {
            Bucket removed = instances.remove(bucket);
            if (removed != null) {
                removed.clear();
            } else {
                Bucket instance = new TCBucket(bucket);
                hydrateBucket(instance);
                instance.clear();
            }
            buckets.putNoReturn(bucket, TOMBSTONE);
        } finally {
            buckets.unlockEntry(bucket);
        }
    }

    @Override
    public Set<String> buckets() {
        Set<String> result = new HashSet<String>();
        Set<Entry<String, String>> entries = buckets.entrySet();
        for (Entry<String, String> keyValue : entries) {
            String bucket = keyValue.getKey();
            buckets.lockEntry(bucket);
            try {
                if (!keyValue.getValue().equals(TOMBSTONE)) {
                    result.add(bucket);
                } else {
                    Bucket instance = instances.get(bucket);
                    if (instance == null) {
                        instance = new TCBucket(bucket);
                        hydrateBucket(instance);
                        instances.put(bucket, instance);
                    }
                    if (instance.size() > 0) {
                        buckets.put(bucket, bucket);
                        result.add(bucket);
                    }
                }
            } finally {
                buckets.unlockEntry(bucket);
            }
        }
        return result;
    }

    @Override
    public Map<String, Object> map(final String bucketName, final Set<Key> keys, final Mapper mapper) throws StoreOperationException {
        try {
            final Bucket bucket = get(bucketName);
            List<Map<String, Object>> mapResults = ParallelUtils.parallelMap(
                    keys,
                    new MapTask<Key, Map<String, Object>>() {

                        @Override
                        public Map<String, Object> map(Key key) {
                            try {
                                LOG.warn("Mapping bucket {} and key {} ...", bucket, key);
                                return bucket.map(key, mapper);
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        }

                    },
                    new MapCollector<Map<String, Object>, List<Map<String, Object>>>() {

                        @Override
                        public List<Map<String, Object>> collect(List<Map<String, Object>> mapResults) {
                            return mapResults;
                        }

                    }, GlobalExecutor.getStoreExecutor());
            Aggregator aggregator = getAggregator(mapper.getCombinerName());
            if (aggregator != null) {
                return aggregate(mapResults, aggregator, mapper.getTimeoutInMillis());
            } else {
                throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Wrong combiner name: " + mapper.getCombinerName()));
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
        }

    }

    @Override
    public Value reduce(List<Map<String, Object>> values, Reducer reducer) throws StoreOperationException {
        Aggregator aggregator = getAggregator(reducer.getReducerName());
        Map<String, Object> aggregation = aggregate(values, aggregator, reducer.getTimeoutInMillis());
        return JsonUtils.fromMap(aggregation);
    }

    @Override
    public void flush(FlushStrategy flushStrategy, FlushCondition flushCondition) {
        for (Bucket bucket : instances.values()) {
            bucket.flush(flushStrategy, flushCondition);
        }
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

    @Override
    public void setAggregators(Map<String, Aggregator> aggregators) {
        this.aggregators.clear();
        this.aggregators.putAll(aggregators);
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
    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    private void hydrateBucket(Bucket bucket) {
        // We need to manually set all of this because of TC not supporting injection ...
        bucket.setDefaultComparator(defaultComparator);
        bucket.setComparators(comparators);
        bucket.setConditions(conditions);
        bucket.setFunctions(functions);
        bucket.setSnapshotManager(snapshotManager);
        bucket.setBackupManager(backupManager);
        bucket.setEventBus(eventBus);
        // TODO: verify this is not a perf problem.
    }

    private Aggregator getAggregator(String aggregatorName) throws StoreOperationException {
        if (aggregators.containsKey(aggregatorName)) {
            return aggregators.get(aggregatorName);
        } else {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Wrong function name: " + aggregatorName));
        }
    }

    private Map<String, Object> aggregate(final List<Map<String, Object>> values, final Aggregator aggregator, long timeout) throws StoreOperationException {
        Future<Map<String, Object>> task = null;
        try {
            task = GlobalExecutor.getStoreExecutor().submit(new Callable<Map<String, Object>>() {

                @Override
                public Map<String, Object> call() {
                    return aggregator.apply(values);
                }

            });
            return task.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            task.cancel(true);
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Aggregation cancelled due to long execution time."));
        } catch (Exception ex) {
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
        }
    }

}
