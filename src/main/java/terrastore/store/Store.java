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
package terrastore.store;

import java.util.List;
import java.util.Map;
import java.util.Set;
import terrastore.event.EventBus;
import terrastore.store.features.Mapper;
import terrastore.store.features.Reducer;
import terrastore.store.operators.Aggregator;
import terrastore.store.operators.Comparator;
import terrastore.store.operators.Condition;
import terrastore.store.operators.Function;

/**
 * Store interface for managing {@link Bucket} instances.
 *
 * @author Sergio Bossa
 */
public interface Store {

    /**
     * Get a named {@link Bucket}, or create it if not existing.
     *
     * @param bucket The bucket name.
     * @return The named bucket.
     */
    public Bucket getOrCreate(String bucket);

    /**
     * Remove a named {@link Bucket}.<br>
     * This operation is idempotent.
     *
     * @param bucket The bucket name.
     */
    public void remove(String bucket);

    /**
     * Get a named {@link Bucket}.
     *
     * @param bucket The bucket name.
     * @return The named bucket, or null if no such a bucket is found.
     */
    public Bucket get(String bucket);

    /**
     * Get all {@link Bucket} names of this store.
     * 
     * @return A collection of {@link Bucket} names belonging to this store.
     */
    public Set<String> buckets();

    /**
     *
     */
    public Map<String, Object> map(Mapper mapper) throws StoreOperationException;

    /**
     *
     */
    public Value reduce(Reducer reducer) throws StoreOperationException;

    /**
     * Flush all key/value entries of all buckets contained into this store.
     * <br>
     * The actual decision whether the key must be flushed or not, is left to the given {@link FlushCondition}.
     *
     * @param flushStrategy The algorithm to execute for flushing keys.
     * @param flushCondition The condition to evaluate for flushing keys.
     */
    public void flush(FlushStrategy flushStrategy, FlushCondition flushCondition);

    /**
     * Set the default {@link terrastore.store.operators.Comparator} used to compare keys when no other comparator is found.
     *
     * @param defaultComparator The default comparator.
     */
    public void setDefaultComparator(Comparator defaultComparator);

    /**
     * Set all supported {@link terrastore.store.operators.Comparator} by name.
     *
     * @param comparators A map of supported comparators.
     */
    public void setComparators(Map<String, Comparator> comparators);

    /**
     * Set all supported {@link terrastore.store.operators.Condition} by name.
     *
     * @param conditions  A map of supported conditions.
     */
    public void setConditions(Map<String, Condition> conditions);

    /**
     * Set all supported {@link terrastore.store.operators.Function}s by name.
     *
     * @param functions  A map of supported functions.
     */
    public void setFunctions(Map<String, Function> functions);

    /**
     * Set all supported {@link terrastore.store.operators.Aggregator}s by name.
     *
     * @param aggregators A map of supported aggregators.
     */
    public void setAggregators(Map<String, Aggregator> aggregators);

    /**
     * Set the {@link terrastore.event.EventBus} instance used for publishing events to {@link terrastore.event.EventListener}s.
     *
     * @param eventBus The {@link terrastore.event.EventBus} instance.
     */
    public void setEventBus(EventBus eventBus);

    /**
     * Set the {@link SnapshotManager} used to compute the snapshot of the keys used in range queries.
     *
     * @param snapshotManager The {@link SnapshotManager} instance.
     */
    public void setSnapshotManager(SnapshotManager snapshotManager);

    /**
     * Set the {@link BackupManager} used to execute export and import of entries from/to buckets.
     *
     * @param backupManager The {@link BackupManager} instance.
     */
    public void setBackupManager(BackupManager backupManager);
}
