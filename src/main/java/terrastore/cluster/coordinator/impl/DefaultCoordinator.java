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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.cluster.ClusterEvent;
import org.terracotta.cluster.ClusterInfo;
import org.terracotta.cluster.ClusterListener;
import org.terracotta.cluster.ClusterNode;
import org.terracotta.cluster.ClusterTopology;
import org.terracotta.collections.ClusteredMap;
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
import terrastore.store.LockManager;
import terrastore.store.Store;
import terrastore.util.concurrent.GlobalExecutor;
import terrastore.util.io.JavaSerializer;
import terrastore.util.io.Serializer;

/**
 * @author Sergio Bossa
 */
// TODO: implement evictor thread for unused locks previously held by dead nodes.
// TODO: implement evictor thread for unused connection table entries previously held by dead nodes.
// Both need patching the terracotta toolkit to access all values belonging to a given map (all node locks, all tables).
public class DefaultCoordinator implements Coordinator, ClusterListener {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultCoordinator.class);
    private static final Serializer SERIALIZER = new JavaSerializer();
    //
    private Lock startLock = TCMaster.getInstance().getReadWriteLock(DefaultCoordinator.class.getName() + ".startLock").writeLock();
    private Lock connectionLock = TCMaster.getInstance().getReadWriteLock(DefaultCoordinator.class.getName() + ".connectionLock").writeLock();
    private Condition connectionCondition = connectionLock.newCondition();
    //
    private volatile ExecutorService connectionExecutor;
    private volatile ExecutorService disconnectionExecutor;
    private volatile ExecutorService reconnectionExecutor;
    private volatile ReentrantLock reconnectionLock;
    private volatile Condition reconnectionCondition;
    private volatile long reconnectTimeout;
    private volatile boolean connected;
    //
    private volatile Cluster thisCluster;
    private volatile ServerConfiguration thisConfiguration;
    private volatile ConcurrentMap<String, Node> clusterNodes;
    private volatile LocalProcessor localProcessor;
    private volatile RemoteProcessor remoteProcessor;
    //
    private volatile boolean compressCommunication;
    private volatile long nodeTimeout;
    private volatile int remoteProcessorThreads;
    private volatile int globalExecutorThreads;
    //
    private volatile LockManager lockManager;
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
    public void setCompressCommunication(boolean compressCommunication) {
        this.compressCommunication = compressCommunication;
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
        int threads = workerThreads / 2;
        this.remoteProcessorThreads = threads;
        this.globalExecutorThreads = threads;
    }

    @Override
    public void setLockManager(LockManager lockManager) {
        this.lockManager = lockManager;
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
        startLock.lock();
        try {
            // Configure local data:
            connectionExecutor = Executors.newSingleThreadExecutor();
            disconnectionExecutor = Executors.newSingleThreadExecutor();
            reconnectionExecutor = Executors.newSingleThreadExecutor();
            reconnectionLock = new ReentrantLock();
            reconnectionCondition = reconnectionLock.newCondition();
            thisCluster = new Cluster(ensembleConfiguration.getLocalCluster(), true);
            thisConfiguration = serverConfiguration;
            clusterNodes = new ConcurrentHashMap<String, Node>();
            // Configure global executor:
            GlobalExecutor.configure(globalExecutorThreads);
            // Setup ensemble:
            setupEnsemble(ensembleConfiguration);
            // Setup shutdown hook:
            setupShutdownHook();
            // Add cluster listener to listen to events:
            getCluster().addClusterListener(this);
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            startLock.unlock();
        }
    }

    public void nodeJoined(final ClusterEvent event) {
        connectionExecutor.submit(new Runnable() {

            @Override
            public void run() {
                String joinedNodeName = ClusterUtils.getServerId(event.getNode());
                if (isThisNode(joinedNodeName)) {
                    try {
                        LOG.info("Joining this node as {}:{}", thisCluster.getName(), thisConfiguration.getName());
                        setupThisNode();
                        setupRemoteNodes();
                        LOG.info("This node is now ready to work as {}:{}", thisCluster.getName(), thisConfiguration.getName());
                    } catch (Exception ex) {
                        LOG.error(ex.getMessage(), ex);
                    }
                } else {
                    try {
                        LOG.info("Joining remote node as {}:{}", thisCluster.getName(), joinedNodeName);
                        pauseProcessing();
                        signalConnection(getNodeConnectionTable(joinedNodeName), thisConfiguration);
                        waitForConnection(getNodeConnectionTable(thisConfiguration.getName()), joinedNodeName);
                        connectRemoteNode(getNodeConnectionTable(thisConfiguration.getName()), joinedNodeName);
                        flushThisNodeKeys();
                        LOG.info("Remote node is now ready to work as {}:{}", thisCluster.getName(), joinedNodeName);
                    } catch (Exception ex) {
                        LOG.error(ex.getMessage(), ex);
                    } finally {
                        resumeProcessing();
                    }
                }
            }

        });
    }

    public void nodeLeft(final ClusterEvent event) {
        disconnectionExecutor.submit(new Runnable() {

            @Override
            public void run() {
                String leftNodeName = ClusterUtils.getServerId(event.getNode());
                if (!isThisNode(leftNodeName) && clusterNodes.containsKey(leftNodeName)) {
                    try {
                        pauseProcessing();
                        clearNodeConnectionTable(getNodeConnectionTable(leftNodeName));
                        disconnectRemoteNode(getNodeConnectionTable(thisConfiguration.getName()), leftNodeName);
                        flushThisNodeKeys();
                    } catch (Exception ex) {
                        LOG.error(ex.getMessage(), ex);
                    } finally {
                        resumeProcessing();
                    }
                }
            }

        });
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

    public void operationsDisabled(ClusterEvent event) {
        if (connected) {
            reconnectionExecutor.submit(new Runnable() {

                @Override
                public void run() {
                    attemptReconnection();
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

            });
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

    private void setupThisNode() {
        // Local Processor:
        localProcessor = new LocalProcessor(router, store);
        localProcessor.start();
        // Local node:
        Node thisNode = localNodeFactory.makeLocalNode(thisConfiguration, localProcessor);
        thisNode.connect();
        clusterNodes.put(thisConfiguration.getName(), thisNode);
        router.addRouteToLocalNode(thisNode);
        router.addRouteTo(thisCluster, thisNode);
        // Remote processor:
        remoteProcessor = new RemoteProcessor(thisConfiguration.getNodeHost(), thisConfiguration.getNodePort(), remoteProcessorThreads, compressCommunication, router);
        remoteProcessor.start();
    }

    private void setupRemoteNodes() throws InterruptedException {
        ClusterTopology dsoTopology = getCluster().getClusterTopology();
        for (ClusterNode dsoNode : dsoTopology.getNodes()) {
            String serverId = ClusterUtils.getServerId(dsoNode);
            if (!isThisNode(serverId)) {
                String remoteNode = serverId;
                ClusteredMap<String, byte[]> remoteTable = getNodeConnectionTable(remoteNode);
                ClusteredMap<String, byte[]> thisTable = getNodeConnectionTable(thisConfiguration.getName());
                signalConnection(remoteTable, thisConfiguration);
                waitForConnection(thisTable, remoteNode);
                connectRemoteNode(thisTable, remoteNode);
                LOG.info("Set up remote node {}:{}", thisCluster.getName(), remoteNode);
            }
        }
    }

    private void connectRemoteNode(ClusteredMap<String, byte[]> connectionTable, String nodeName) throws InterruptedException {
        ServerConfiguration remoteConfiguration = (ServerConfiguration) SERIALIZER.deserialize(connectionTable.get(nodeName));
        if (remoteConfiguration != null) {
            Node remoteNode = remoteNodeFactory.makeRemoteNode(remoteConfiguration, nodeTimeout, compressCommunication);
            remoteNode.connect();
            clusterNodes.put(nodeName, remoteNode);
            router.addRouteTo(thisCluster, remoteNode);
        } else {
            LOG.warn("Cannot set up remote node {}", nodeName);
        }
    }

    private void waitForConnection(ClusteredMap<String, byte[]> connectionTable, String nodeName) throws InterruptedException {
        connectionLock.lock();
        try {
            int times = 0;
            while (!connectionTable.containsKey(nodeName) && !isPrematurelyDead(nodeName)) {
                connectionCondition.await(1000, TimeUnit.MILLISECONDS);
                times++;
                // FIXME: "times" should be configurable ...
                if (times > 100) {
                    LOG.warn("Detected excessive waiting for node: {}", nodeName);
                }
            }
        } finally {
            connectionLock.unlock();
        }
    }

    private void signalConnection(ClusteredMap<String, byte[]> connectionTable, ServerConfiguration configuration) {
        connectionLock.lock();
        try {
            connectionTable.put(configuration.getName(), SERIALIZER.serialize(configuration));
            connectionCondition.signalAll();
        } finally {
            connectionLock.unlock();
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
        LOG.warn("About to flush keys on this node {}", thisConfiguration.getName());
        store.flush(flushStrategy, flushCondition);
        LOG.warn("Finished flushing keys on this node {}", thisConfiguration.getName());
    }

    private void disconnectRemoteNode(ClusteredMap<String, byte[]> connectionTable, String nodeName) {
        Node remoteNode = clusterNodes.remove(nodeName);
        remoteNode.disconnect();
        connectionTable.removeNoReturn(nodeName);
        router.removeRouteTo(thisCluster, remoteNode);
        LOG.info("Disconnected node {}:{}", thisCluster.getName(), nodeName);
    }

    private void shutdownEverything() {
        for (Node node : clusterNodes.values()) {
            node.disconnect();
        }
        localProcessor.stop();
        remoteProcessor.stop();
        ensembleManager.shutdown();
        connectionExecutor.shutdown();
        disconnectionExecutor.shutdown();
        reconnectionExecutor.shutdown();
        GlobalExecutor.shutdown();
    }

    private void cleanupEverything() {
        clusterNodes.clear();
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

    private void attemptReconnection() {
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

    private void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                if (connected) {
                    try {
                        LOG.info("Shutting down this node {}:{}", thisCluster.getName(), thisConfiguration.getName());
                        shutdownEverything();
                        cleanupEverything();
                    } catch (Exception ex) {
                        LOG.error(ex.getMessage(), ex);
                    }
                }
            }

        });
    }

    private ClusteredMap<String, byte[]> getNodeConnectionTable(String node) {
        return TCMaster.getInstance().getAutolockedMap(DefaultCoordinator.class.getName() + ".connectionsTable." + node);
    }

    private void clearNodeConnectionTable(ClusteredMap<String, byte[]> connectionTable) {
        connectionTable.clear();
    }

    private boolean isPrematurelyDead(String node) {
        ClusterTopology dsoTopology = getCluster().getClusterTopology();
        boolean dead = true;
        for (ClusterNode dsoNode : dsoTopology.getNodes()) {
            String serverId = ClusterUtils.getServerId(dsoNode);
            if (serverId.equals(node)) {
                dead = false;
                break;
            }
        }
        return dead;
    }

}
