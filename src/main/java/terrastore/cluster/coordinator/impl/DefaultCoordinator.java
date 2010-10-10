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
package terrastore.cluster.coordinator.impl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import jsr166y.ForkJoinPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.cluster.ClusterEvent;
import org.terracotta.cluster.ClusterInfo;
import org.terracotta.cluster.ClusterListener;
import org.terracotta.cluster.ClusterNode;
import org.terracotta.cluster.ClusterTopology;
import org.terracotta.util.ClusteredAtomicLong;
import terrastore.cluster.ClusterUtils;
import terrastore.communication.Node;
import terrastore.cluster.coordinator.Coordinator;
import terrastore.cluster.coordinator.ServerConfiguration;
import terrastore.communication.ProcessingException;
import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.communication.Cluster;
import terrastore.communication.LocalNodeFactory;
import terrastore.communication.RemoteNodeFactory;
import terrastore.router.MissingRouteException;
import terrastore.store.FlushCondition;
import terrastore.store.FlushStrategy;
import terrastore.communication.local.LocalProcessor;
import terrastore.communication.remote.RemoteProcessor;
import terrastore.cluster.ensemble.EnsembleManager;
import terrastore.internal.tc.TCMaster;
import terrastore.router.Router;
import terrastore.store.Store;
import terrastore.util.global.GlobalExecutor;
import terrastore.util.io.JavaSerializer;
import terrastore.util.io.Serializer;

/**
 * @author Sergio Bossa
 */
public class DefaultCoordinator implements Coordinator, ClusterListener {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultCoordinator.class);
    //
    private Lock stateLock = TCMaster.getInstance().getReadWriteLock(DefaultCoordinator.class.getName() + ".stateLock").writeLock();
    private ClusteredAtomicLong membershipCounter = TCMaster.getInstance().getLong(DefaultCoordinator.class.getName() + ".membershipCounter");
    private Map<String, byte[]> addressTable = TCMaster.getInstance().getMap(DefaultCoordinator.class.getName() + ".addressTable");
    private Condition setupAddressCondition = stateLock.newCondition();
    private Condition setupMembershipCondition = stateLock.newCondition();
    //
    private Serializer<ServerConfiguration> javaSerializer = new JavaSerializer<ServerConfiguration>();
    //
    private volatile ReentrantLock reconnectionLock;
    private volatile Condition reconnectionCondition;
    private volatile long reconnectTimeout;
    private volatile boolean connected;
    //
    private volatile Cluster thisCluster;
    private volatile ServerConfiguration thisConfiguration;
    private volatile ConcurrentMap<String, Node> nodes;
    private volatile LocalProcessor localProcessor;
    private volatile RemoteProcessor remoteProcessor;
    //
    private volatile int maxFrameLength;
    private volatile long nodeTimeout;
    private volatile int remoteProcessorThreads;
    private volatile int globalExecutorThreads;
    private volatile int fjThreads;
    private volatile ExecutorService globalExecutor;
    private volatile ForkJoinPool globalFJPool;
    //
    private volatile Store store;
    private volatile Router router;
    private volatile EnsembleManager ensembleManager;
    private volatile LocalNodeFactory localNodeFactory;
    private volatile RemoteNodeFactory remoteNodeFactory;
    private volatile FlushStrategy flushStrategy;
    private volatile FlushCondition flushCondition;

    public DefaultCoordinator() {
    }

    @Override
    public void setReconnectTimeout(long reconnectTimeout) {
        this.reconnectTimeout = reconnectTimeout;
    }

    @Override
    public void setNodeTimeout(long nodeTimeout) {
        this.nodeTimeout = nodeTimeout;
    }

    @Override
    public void setWokerThreads(int workerThreads) {
        int threads = workerThreads / 3;
        this.remoteProcessorThreads = threads;
        this.globalExecutorThreads = threads;
        this.fjThreads = threads;
    }

    @Override
    public void setMaxFrameLength(int maxFrameLength) {
        this.maxFrameLength = maxFrameLength;
    }

    @Override
    public void setStore(Store store) {
        this.store = store;
    }

    @Override
    public void setRouter(Router router) {
        this.router = router;
    }

    @Override
    public void setEnsembleManager(EnsembleManager ensembleManager) {
        this.ensembleManager = ensembleManager;
    }

    @Override
    public void setLocalNodeFactory(LocalNodeFactory localNodeFactory) {
        this.localNodeFactory = localNodeFactory;
    }

    @Override
    public void setRemoteNodeFactory(RemoteNodeFactory remoteNodeFactory) {
        this.remoteNodeFactory = remoteNodeFactory;
    }

    @Override
    public void setFlushStrategy(FlushStrategy flushStrategy) {
        this.flushStrategy = flushStrategy;
    }

    @Override
    public void setFlushCondition(FlushCondition flushCondition) {
        this.flushCondition = flushCondition;
    }

    public void start(ServerConfiguration serverConfiguration, EnsembleConfiguration ensembleConfiguration) {
        stateLock.lock();
        try {
            // Configure local data:
            reconnectionLock = new ReentrantLock();
            reconnectionCondition = reconnectionLock.newCondition();
            thisCluster = new Cluster(ensembleConfiguration.getLocalCluster(), true);
            thisConfiguration = serverConfiguration;
            // Configure transients:
            nodes = new ConcurrentHashMap<String, Node>();
            // Configure global task executor and fj pool:
            globalExecutor = Executors.newFixedThreadPool(globalExecutorThreads);
            globalFJPool = new ForkJoinPool(fjThreads);
            GlobalExecutor.setExecutor(globalExecutor);
            GlobalExecutor.setForkJoinPool(globalFJPool);
            // Setup ensemble:
            setupEnsemble(ensembleConfiguration);
            // Add cluster listener to listen to events:
            getCluster().addClusterListener(this);
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            stateLock.unlock();
        }
    }

    public void operationsEnabled(ClusterEvent event) {
        reconnectionLock.lock();
        try {
            connected = true;
            reconnectionCondition.signalAll();
        } finally {
            reconnectionLock.unlock();
        }
    }

    public void nodeJoined(ClusterEvent event) {
        String joinedNodeName = ClusterUtils.getServerId(event.getNode());
        if (isThisNode(joinedNodeName)) {
            stateLock.lock();
            try {
                LOG.info("Joining this node {}:{}", thisCluster.getName(), thisConfiguration.getName());
                startMembershipChange();
                setupThisNode();
                setupThisRemoteProcessor();
                setupAddressTable();
                setupRemoteNodes();
                completeMembershipChange();
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
            } finally {
                stateLock.unlock();
            }
        } else {
            stateLock.lock();
            try {
                LOG.info("Joining remote node {}:{}", thisCluster.getName(), joinedNodeName);
                pauseProcessing();
                startMembershipChange();
                connectRemoteNode(joinedNodeName);
                flushThisNodeKeys();
                completeMembershipChange();
                resumeProcessing();
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
            } finally {
                stateLock.unlock();
            }
        }
    }

    public void nodeLeft(ClusterEvent event) {
        String leftNodeName = ClusterUtils.getServerId(event.getNode());
        if (!isThisNode(leftNodeName)) {
            stateLock.lock();
            try {
                pauseProcessing();
                startMembershipChange();
                disconnectRemoteNode(leftNodeName);
                flushThisNodeKeys();
                completeMembershipChange();
                resumeProcessing();
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
            } finally {
                stateLock.unlock();
            }
        }
    }

    public void operationsDisabled(ClusterEvent event) {
        if (connected) {
            new Thread() {

                @Override
                public void run() {
                    connected = false;
                    reconnectionLock.lock();
                    try {
                        LOG.info("Attempting reconnection for this node {}:{}", thisCluster.getName(), thisConfiguration.getName());
                        long timeoutInNanos = TimeUnit.MILLISECONDS.toNanos(reconnectTimeout);
                        while (!connected) {
                            if (timeoutInNanos > 0) {
                                timeoutInNanos = reconnectionCondition.awaitNanos(timeoutInNanos);
                            } else {
                                break;
                            }
                        }
                    } catch (InterruptedException ex) {
                        // abort
                    } finally {
                        reconnectionLock.unlock();
                    }
                    if (!connected) {
                        try {
                            LOG.info("Disabling this node {}:{}", thisCluster.getName(), thisConfiguration.getName());
                            shutdownEverything();
                            cleanupEverything();
                        } catch (Exception ex) {
                            LOG.error(ex.getMessage(), ex);
                        } finally {
                            doExit();
                        }
                    } else {
                        LOG.info("Successful reconnection for this node {}:{}", thisCluster.getName(), thisConfiguration.getName());
                    }
                }
            }.start();
        }
    }

    private ClusterInfo getCluster() {
        ClusterInfo cluster = TCMaster.getInstance().getClusterInfo();
        if (cluster != null) {
            return cluster;
        } else {
            throw new IllegalStateException("No cluster found!");
        }
    }

    private boolean isThisNode(String candidateNodeName) {
        return thisConfiguration.getName().equals(candidateNodeName);
    }

    private void setupThisRemoteProcessor() {
        remoteProcessor = new RemoteProcessor(thisConfiguration.getNodeHost(), thisConfiguration.getNodePort(), maxFrameLength, remoteProcessorThreads, router);
        remoteProcessor.start();
        LOG.debug("Set up processor for {}", thisConfiguration.getName());
    }

    private void setupThisNode() {
        localProcessor = new LocalProcessor(router, store);
        Node thisNode = localNodeFactory.makeLocalNode(thisConfiguration, localProcessor);
        localProcessor.start();
        thisNode.connect();
        nodes.put(thisConfiguration.getName(), thisNode);
        router.addRouteToLocalNode(thisNode);
        router.addRouteTo(thisCluster, thisNode);
        LOG.info("Set up this node {}:{}", thisCluster.getName(), thisConfiguration.getName());
    }

    private void setupAddressTable() {
        addressTable.put(thisConfiguration.getName(), javaSerializer.serialize(thisConfiguration));
        setupAddressCondition.signalAll();
    }

    private void setupRemoteNodes() throws InterruptedException {
        ClusterTopology dsoTopology = getCluster().getClusterTopology();
        for (ClusterNode dsoNode : dsoTopology.getNodes()) {
            String serverId = ClusterUtils.getServerId(dsoNode);
            if (!isThisNode(serverId)) {
                String remoteNodeName = serverId;
                connectRemoteNode(remoteNodeName);
            }
        }
    }

    private void connectRemoteNode(String remoteNodeName) throws InterruptedException {
        while (!addressTable.containsKey(remoteNodeName)) {
            setupAddressCondition.await(1000, TimeUnit.MILLISECONDS);
        }
        ServerConfiguration remoteConfiguration = javaSerializer.deserialize(addressTable.get(remoteNodeName));
        if (remoteConfiguration != null) {
            // Double check to tolerate duplicated node joins by terracotta server:
            if (!nodes.containsKey(remoteNodeName)) {
                Node remoteNode = remoteNodeFactory.makeRemoteNode(remoteConfiguration, maxFrameLength, nodeTimeout);
                remoteNode.connect();
                nodes.put(remoteNodeName, remoteNode);
                router.addRouteTo(thisCluster, remoteNode);
                LOG.info("Set up remote node {}:{}", thisCluster.getName(), remoteNodeName);
            }
        } else {
            LOG.warn("Cannot set up remote node {}", remoteNodeName);
        }
    }

    private void pauseProcessing() {
        localProcessor.pause();
        remoteProcessor.pause();
    }

    private void resumeProcessing() {
        localProcessor.resume();
        remoteProcessor.resume();
    }

    private void flushThisNodeKeys() {
        LOG.debug("About to flush keys on node {}:{}", thisCluster.getName(), thisConfiguration.getName());
        store.flush(flushStrategy, flushCondition);
    }

    private void disconnectRemoteNode(String nodeName) {
        Node remoteNode = nodes.remove(nodeName);
        remoteNode.disconnect();
        router.removeRouteTo(thisCluster, remoteNode);
        LOG.info("Disconnected node {}:{}", thisCluster.getName(), nodeName);
    }

    private void shutdownEverything() {
        for (Node node : nodes.values()) {
            node.disconnect();
        }
        localProcessor.stop();
        remoteProcessor.stop();
        ensembleManager.shutdown();
        globalExecutor.shutdown();
        globalFJPool.shutdown();
    }

    private void cleanupEverything() {
        nodes.clear();
        router.cleanup();
    }

    private void doExit() {
        try {
            Thread.sleep(1000);
        } catch (Exception ex) {
        } finally {
            LOG.info("Exiting this node {}:{}", thisCluster.getName(), thisConfiguration.getName());
            System.exit(0);
        }
    }

    private void setupEnsemble(EnsembleConfiguration ensembleConfiguration) throws MissingRouteException, ProcessingException {
        Map<String, Cluster> clusters = new HashMap<String, Cluster>();
        for (String cluster : ensembleConfiguration.getClusters()) {
            if (!cluster.equals(thisCluster.getName())) {
                LOG.info("Set up remote cluster {}", cluster);
                clusters.put(cluster, new Cluster(cluster, false));
            } else {
                LOG.info("Set up this cluster {}", cluster);
                clusters.put(cluster, thisCluster);
            }
        }
        router.setupClusters(new HashSet<Cluster>(clusters.values()));
        for (Map.Entry<String, String> entry : ensembleConfiguration.getSeeds().entrySet()) {
            String cluster = entry.getKey();
            String seed = entry.getValue();
            LOG.info("Joining remote cluster {} with seed {}", cluster, seed);
            ensembleManager.join(clusters.get(cluster), seed, ensembleConfiguration);
        }
    }

    private void startMembershipChange() {
        membershipCounter.compareAndSet(0, getCluster().getClusterTopology().getNodes().size());
        membershipCounter.decrementAndGet();
    }

    private void completeMembershipChange() {
        if (membershipCounter.get() > 0) {
            while (membershipCounter.get() > 0) {
                try {
                    setupMembershipCondition.await();
                } catch (Exception ex) {
                }
            }
        } else {
            setupMembershipCondition.signalAll();
        }
    }

    private static class Address implements Serializable {

        private String host;
        private int port;

        public static String toString(Address address) {
            return address.getHost() + ":" + address.getPort();
        }

        public static Address fromString(String address) {
            String[] parts = address.split(":");
            return new Address(parts[0], Integer.parseInt(parts[1]));
        }

        public Address(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }
}
