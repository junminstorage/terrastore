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

import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.collections.ClusteredMap;
import terrastore.internal.tc.TCMaster;
import terrastore.store.Key;
import terrastore.store.LockManager;

/**
 * Terracotta-based lock manager implementation.
 *
 * @author Sergio Bossa
 */
public class TCLockManager implements LockManager {

    private static final Logger LOG = LoggerFactory.getLogger(TCLockManager.class);
    //
    private final int concurrencyLevel;
    private final ReadWriteLock[] locks;

    public TCLockManager(String node, int concurrencyLevel) {
        this.concurrencyLevel = concurrencyLevel;
        this.locks = new ReadWriteLock[concurrencyLevel];
        ClusteredMap<String, ReadWriteLock> locksMap = TCMaster.getInstance().getAutolockedMap(TCLockManager.class.getName() + ".CLUSTER_LOCKS." + node);
        for (int i = 0; i < concurrencyLevel; i++) {
            String name = node + ":" + i;
            ReadWriteLock lock = TCMaster.getInstance().getReadWriteLock(name);
            locksMap.put(name, lock);
            locks[i] = lock;
            LOG.debug("Created lock {}", name);
        }
    }

    @Override
    public void evictLocks(String node) {
        ReadWriteLock evictorLock = TCMaster.getInstance().getReadWriteLock(TCLockManager.class.getName() + ".EVICTOR");
        evictorLock.writeLock().lock();
        try {
            Map<String, ReadWriteLock> nodeLocks = TCMaster.getInstance().<String, ReadWriteLock>getAutolockedMap(TCLockManager.class.getName() + ".CLUSTER_LOCKS." + node);
            for (String lock : nodeLocks.keySet()) {
                TCMaster.getInstance().evictReadWriteLock(lock);
                LOG.debug("Evicted lock {}", lock);
            }
            nodeLocks.clear();
            // TODO: evict the map itself too.
        } finally {
            evictorLock.writeLock().unlock();
        }
    }

    @Override
    public void lockRead(String bucket, Key key) {
        ReadWriteLock lock = lockFor(bucket, key);
        lock.readLock().lock();
    }

    @Override
    public void unlockRead(String bucket, Key key) {
        ReadWriteLock lock = lockFor(bucket, key);
        lock.readLock().unlock();
    }

    @Override
    public void lockWrite(String bucket, Key key) {
        ReadWriteLock lock = lockFor(bucket, key);
        lock.writeLock().lock();
    }

    @Override
    public void unlockWrite(String bucket, Key key) {
        ReadWriteLock lock = lockFor(bucket, key);
        lock.writeLock().unlock();
    }

    private ReadWriteLock lockFor(String bucket, Key key) {
        String name = bucket + ":" + key;
        return locks[Math.abs(name.hashCode()) % concurrencyLevel];
    }

}
