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

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.terracotta.collections.ClusteredMap;
import terrastore.internal.tc.TCMaster;
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
public class TCStore implements Store {

    // TODO: buckets marked with tombstones aren't currently removed, only cleared.
    private final static String TOMBSTONE = TCStore.class.getName() + ".TOMBSTONE";
    //
    private final ClusteredMap<String, String> buckets;
    private final ConcurrentMap<String, Bucket> instances;
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
    public void flush(FlushStrategy flushStrategy, FlushCondition flushCondition) {
        for (Bucket bucket : instances.values()) {
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
