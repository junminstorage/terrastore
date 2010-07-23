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
import org.terracotta.annotations.HonorTransient;
import org.terracotta.annotations.InstrumentedClass;
import org.terracotta.annotations.Root;
import org.terracotta.collections.ConcurrentDistributedMap;
import org.terracotta.locking.LockType;
import org.terracotta.locking.strategy.HashcodeLockStrategy;
import terrastore.event.EventBus;
import terrastore.store.BackupManager;
import terrastore.store.Bucket;
import terrastore.store.FlushCondition;
import terrastore.store.FlushStrategy;
import terrastore.store.SnapshotManager;
import terrastore.store.Store;

/**
 * @author Sergio Bossa
 */
@InstrumentedClass
@HonorTransient
public class TCStore implements Store {

    @Root
    private static final TCStore INSTANCE = new TCStore();
    //
    private final ConcurrentDistributedMap<String, Bucket> buckets;
    private transient volatile SnapshotManager snapshotManager;
    private transient volatile BackupManager backupManager;
    private transient volatile EventBus eventBus;

    public static TCStore getInstance() {
        return INSTANCE;
    }

    public TCStore() {
        buckets = new ConcurrentDistributedMap<String, Bucket>(LockType.WRITE, new HashcodeLockStrategy());
    }

    @Override
    public Bucket getOrCreate(String bucket) {
        Bucket requested = buckets.unlockedGet(bucket);
        requested = requested != null ? requested : buckets.get(bucket);
        if (requested == null) {
            buckets.lockEntry(bucket);
            try {
                requested = buckets.get(bucket);
                if (requested == null) {
                    Bucket created = new TCBucket(bucket);
                    buckets.putNoReturn(bucket, created);
                    requested = created;
                }
            } finally {
                buckets.unlockEntry(bucket);
            }
        }
        hydrateBucket(requested);
        return requested;
    }

    @Override
    public void remove(String bucket) {
        buckets.remove(bucket);
    }

    public Bucket get(String bucket) {
        Bucket requested = buckets.unlockedGet(bucket);
        requested = requested != null ? requested : buckets.get(bucket);
        if (requested != null) {
            hydrateBucket(requested);
            return requested;
        } else {
            return null;
        }
    }

    @Override
    public Collection<Bucket> buckets() {
        return buckets.values();
    }

    @Override
    public void flush(FlushStrategy flushStrategy, FlushCondition flushCondition) {
        for (Bucket bucket : buckets.values()) {
            bucket.flush(flushStrategy, flushCondition);
        }
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

    private void hydrateBucket(Bucket requested) {
        // We need to manually set the event bus because of TC not supporting injection ...
        requested.setSnapshotManager(snapshotManager);
        requested.setBackupManager(backupManager);
        requested.setEventBus(eventBus);
        // TODO: verify this is not a perf problem.
    }
}
