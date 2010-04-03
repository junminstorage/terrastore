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

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import terrastore.event.EventBus;

/**
 * Store interface for managing {@link Bucket} instances.
 *
 * @author Sergio Bossa
 */
public interface Store {

    /**
     * Add a named {@link Bucket}.<br>
     * This operation is idempotent.
     *
     * @param bucket The bucket name, which must be unique.
     */
    public void add(String bucket);

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
     * @return The named bucket.
     * @throws StoreOperationException If the bucket doesn't exist.
     */
    public Bucket get(String bucket) throws StoreOperationException;

    /**
     *Get all {@link Bucket}s of this store.
     * 
     * @return A collection of {@link Bucket}s.
     */
    public Collection<Bucket> buckets();

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

    /**
     * Set the executor to use for asynchronously running tasks.
     *
     * @param taskExecutor The executor.
     */
    public void setTaskExecutor(ExecutorService taskExecutor);
}
