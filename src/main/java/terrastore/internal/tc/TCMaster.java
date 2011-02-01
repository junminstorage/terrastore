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
package terrastore.internal.tc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.api.ClusteringToolkit;
import org.terracotta.api.TerracottaClient;
import org.terracotta.cluster.ClusterInfo;
import org.terracotta.collections.ClusteredMap;
import org.terracotta.collections.ConcurrentDistributedMap;
import org.terracotta.coordination.Barrier;
import org.terracotta.locking.ClusteredLock;
import org.terracotta.locking.LockStrategy;
import org.terracotta.locking.LockType;
import org.terracotta.util.ClusteredAtomicLong;
import org.terracotta.util.ClusteredTextBucket;
import org.terracotta.util.TerracottaAtomicLong;

/**
 * @author Sergio Bossa
 */
public class TCMaster {

    private static final Logger LOG = LoggerFactory.getLogger(TCMaster.class);
    //
    private static final TCMaster INSTANCE = new TCMaster();
    //
    private volatile boolean connected;
    private volatile ClusteringToolkit toolkit;

    public static TCMaster getInstance() {
        return INSTANCE;
    }

    protected TCMaster() {
        connected = false;
        toolkit = new LocalToolkit();
    }

    public boolean connect(String url, long timeout, TimeUnit unit) {
        if (!connected) {
            FutureTask<ClusteringToolkit> futureTask = null;
            try {
                futureTask = startConnector(url);
                toolkit = futureTask.get(timeout, unit);
                connected = true;
            } catch (InterruptedException ex) {
                LOG.error(ex.getMessage(), ex);
                toolkit = new LocalToolkit();
                connected = false;
            } catch (ExecutionException ex) {
                LOG.error(ex.getMessage(), ex);
                toolkit = new LocalToolkit();
                connected = false;
            } catch (TimeoutException ex) {
                LOG.error(ex.getMessage(), ex);
                futureTask.cancel(true);
                toolkit = new LocalToolkit();
                connected = false;
            }
        } else {
            throw new IllegalStateException("Already connected!");
        }
        return connected;
    }

    public void setupLocally() {
        connected = false;
        toolkit = new LocalToolkit();
    }

    public ReadWriteLock getReadWriteLock(String name) {
        return toolkit.getReadWriteLock(name);
    }

    public void evictReadWriteLock(String name) {
        toolkit.evictReadWriteLock(name);
    }

    public <K, V> ClusteredMap<K, V> getAutolockedMap(String name) {
        return toolkit.getMap(name, LockType.WRITE, LockStrategy.HASH_CODE);
    }

    public <K, V> ClusteredMap<K, V> getUnlockedMap(String name) {
        return toolkit.getMap(name, LockType.WRITE, LockStrategy.NULL);
    }

    public ClusteredAtomicLong getLong(String name) {
        return toolkit.getAtomicLong(name);
    }

    public ClusterInfo getClusterInfo() {
        return toolkit.getClusterInfo();
    }

    private FutureTask<ClusteringToolkit> startConnector(final String url) {
        FutureTask<ClusteringToolkit> futureTask = new FutureTask<ClusteringToolkit>(new Callable<ClusteringToolkit>() {

            @Override
            public ClusteringToolkit call() throws Exception {
                return new TerracottaClient(url).getToolkit();
            }

        });
        Thread thread = new Thread(futureTask);
        thread.start();
        return futureTask;
    }

    private static class LocalToolkit implements ClusteringToolkit {

        private final Map<String, ReadWriteLock> locks = new HashMap<String, ReadWriteLock>();
        private final Map<String, ClusteredMap> maps = new HashMap<String, ClusteredMap>();
        private final Map<String, ClusteredAtomicLong> numbers = new HashMap<String, ClusteredAtomicLong>();

        @Override
        public synchronized ReadWriteLock getReadWriteLock(String name) {
            if (locks.containsKey(name)) {
                return locks.get(name);
            } else {
                ReadWriteLock lock = new ReentrantReadWriteLock();
                locks.put(name, lock);
                return lock;
            }
        }

        @Override
        public void evictReadWriteLock(String name) {
            locks.remove(name);
        }

        @Override
        public synchronized <K, V> ClusteredMap<K, V> getMap(String name) {
            if (maps.containsKey(name)) {
                return maps.get(name);
            } else {
                ClusteredMap<K, V> map = new ConcurrentDistributedMap<K, V>();
                maps.put(name, map);
                return map;
            }
        }

        @Override
        public <K, V> ClusteredMap<K, V> getMap(String name, LockType lockType, String lockStrategy) {
            try {
                if (maps.containsKey(name)) {
                    return maps.get(name);
                } else {
                    ClusteredMap<K, V> map = new ConcurrentDistributedMap<K, V>(lockType, (LockStrategy<? super K>) Class.forName(lockStrategy).
                            newInstance());
                    maps.put(name, map);
                    return map;
                }
            } catch (Exception ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
            }
        }

        @Override
        public synchronized ClusteredAtomicLong getAtomicLong(String name) {
            if (numbers.containsKey(name)) {
                return numbers.get(name);
            } else {
                ClusteredAtomicLong number = new TerracottaAtomicLong(0);
                numbers.put(name, number);
                return number;
            }
        }

        @Override
        public ClusterInfo getClusterInfo() {
            return null;
        }

        @Override
        public Barrier getBarrier(String name, int parties) throws IllegalArgumentException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ClusteredTextBucket getTextBucket(String name) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public <E> BlockingQueue<E> getBlockingQueue(String name) throws IllegalArgumentException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public <E> BlockingQueue<E> getBlockingQueue(String name, int capacity) throws IllegalArgumentException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public <E> List<E> getList(String name) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ClusteredLock createLock(Object monitor, LockType type) throws IllegalArgumentException, NullPointerException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

    }
}
