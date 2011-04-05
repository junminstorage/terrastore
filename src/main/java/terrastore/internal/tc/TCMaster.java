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
import org.terracotta.api.ClusteringToolkitExtension;
import org.terracotta.api.TerracottaClient;
import org.terracotta.cluster.ClusterInfo;
import org.terracotta.collections.ClusteredMap;
import org.terracotta.collections.ConcurrentDistributedMap;
import org.terracotta.coordination.Barrier;
import org.terracotta.express.Client;
import org.terracotta.express.ClientFactory;
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
    private static final String TERRASTORE_SHARED_ROOT = "TERRASTORE_ROOT";
    //
    private volatile ClusteringToolkitExtension toolkit;
    private volatile Client client;

    public static TCMaster getInstance() {
        return INSTANCE;
    }

    protected TCMaster() {
        toolkit = new LocalToolkit();
    }

    public boolean connect(String url, long timeout, TimeUnit unit) {
        if (client == null) {
            FutureTask<Client> futureTask = null;
            try {
                futureTask = startConnector(url);
                client = futureTask.get(timeout, unit);
                toolkit = ClusteredType.TerracottaToolkit.newInstance(client);
            } catch (InterruptedException ex) {
                LOG.error(ex.getMessage(), ex);
                toolkit = new LocalToolkit();
            } catch (ExecutionException ex) {
                LOG.error(ex.getMessage(), ex);
                toolkit = new LocalToolkit();
            } catch (TimeoutException ex) {
                LOG.error(ex.getMessage(), ex);
                futureTask.cancel(true);
                toolkit = new LocalToolkit();
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
                toolkit = new LocalToolkit();
            }
        } else {
            throw new IllegalStateException("Already connected!");
        }
        return client != null;
    }

    public void setupLocally() {
        toolkit = new LocalToolkit();
    }

    public ReadWriteLock getReadWriteLock(String name) {
        return toolkit.getReadWriteLock(name);
    }

    public void evictReadWriteLock(String name) {
        toolkit.unregisterReadWriteLock(name);
    }

    public <K, V> ClusteredMap<K, V> getAutolockedMap(String name) {
        return getOrCreateMap(name, ClusteredType.HashCodeLockStrategy);
    }

    public <K, V> ClusteredMap<K, V> getUnlockedMap(String name) {
        return getOrCreateMap(name, ClusteredType.NullLockStrategy);
    }

    private <K, V> ClusteredMap<K, V> getOrCreateMap(final String name, final ClusteredType lockStrategyType) {
        ClusteredMap<K, V> value = null;
        if (client != null) {
            ClusteredMap<String, ClusteredMap<K, V>> terrastoreRoot = toolkit.getMap(TERRASTORE_SHARED_ROOT);
            value = terrastoreRoot.get(name);
            if (value == null) {
                value = ClusteredType.ConcurrentDistributedServerMap.newInstance(client, LockType.WRITE, lockStrategyType.<LockStrategy<K>>newInstance(client));
                ClusteredMap<K, V> prev = terrastoreRoot.putIfAbsent(name, value);
                if (prev != null) {
                    value = prev;
                }
            }
        } else {
            value = ((LocalToolkit) toolkit).getMap(name, LockType.WRITE, lockStrategyType.className);
        }
        return value;
    }

    public ClusteredAtomicLong getLong(String name) {
        return toolkit.getAtomicLong(name);
    }

    public ClusterInfo getClusterInfo() {
        return toolkit.getClusterInfo();
    }

    private FutureTask<Client> startConnector(final String url) {
        FutureTask<Client> futureTask = new FutureTask<Client>(
                new Callable<Client>() {

                    @Override
                    public Client call() throws Exception {
                        return ClientFactory.getOrCreateClient(url, true, new Class[]{TerracottaClient.class});
                    }

                });
        Thread thread = new Thread(futureTask);
        thread.start();
        return futureTask;
    }

    private enum ClusteredType {

        NullLockStrategy("org.terracotta.locking.strategy.NullLockStrategy"),
        HashCodeLockStrategy("org.terracotta.locking.strategy.HashcodeLockStrategy"),
        ConcurrentDistributedServerMap("org.terracotta.collections.ConcurrentDistributedServerMap", new Class[]{LockType.class, LockStrategy.class}),
        TerracottaToolkit("org.terracotta.api.TerracottaToolkit");
        private final String className;
        private final Class[] constructorTypes;

        private ClusteredType(String className, Class... constructorTypes) {
            this.className = className;
            this.constructorTypes = constructorTypes;
        }

        <T> T newInstance(Client client, Object... params) {
            try {
                return (T) client.instantiate(className, constructorTypes, params);
            } catch (Exception e) {
                throw new RuntimeException("Error instantiating " + className, e);
            }
        }

    }

    private static class LocalToolkit implements ClusteringToolkitExtension {

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
        public synchronized void unregisterReadWriteLock(String name) {
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

        public synchronized <K, V> ClusteredMap<K, V> getMap(String name, LockType lockType, String lockStrategy) {
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

        @Override
        public void unregisterBarrier(final String s) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void unregisterTextBucket(final String s) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void unregisterBlockingQueue(final String s) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void unregisterAtomicLong(final String s) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void unregisterMap(final String s) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void unregisterList(final String s) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

    }
}
