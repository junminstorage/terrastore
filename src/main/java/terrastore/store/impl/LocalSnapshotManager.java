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

import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import terrastore.store.Bucket;
import terrastore.store.SnapshotManager;
import terrastore.store.SortedSnapshot;

/**
 * @author Sergio Bossa
 */
public class LocalSnapshotManager implements SnapshotManager {

    private ConcurrentMap<String, SortedSnapshot> snapshots;
    private ReentrantLock computationLock;

    public LocalSnapshotManager() {
        this.snapshots = new ConcurrentHashMap<String, SortedSnapshot>();
        this.computationLock = new ReentrantLock(true);
    }

    @Override
    public SortedSnapshot getOrComputeSortedSnapshot(Bucket bucket, Comparator<String> comparator, String name, long timeToLive) {
        SortedSnapshot snapshot = snapshots.get(name);
        while (snapshot == null || snapshot.isExpired(timeToLive)) {
            snapshot = tryComputingSnapshot(bucket, comparator, name);
            if (snapshot == null) {
                snapshot = waitForSnapshot(name);
            } else {
                break;
            }
        }
        return snapshot;
    }

    private SortedSnapshot waitForSnapshot(String name) {
        this.computationLock.lock();
        try {
            return snapshots.get(name);
        } finally {
            computationLock.unlock();
        }
    }

    private SortedSnapshot tryComputingSnapshot(Bucket bucket, Comparator<String> comparator, String name) {
        boolean locked = this.computationLock.tryLock();
        if (locked) {
            try {
                Set<String> keys = bucket.keys();
                SortedSnapshot snapshot = new SortedSnapshot(keys, comparator);
                snapshots.put(name, snapshot);
                return snapshot;
            } finally {
                computationLock.unlock();
            }
        } else {
            return null;
        }
    }
}
