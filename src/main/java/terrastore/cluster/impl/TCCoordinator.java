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
package terrastore.cluster.impl;

import com.tc.cluster.DsoCluster;
import com.tc.cluster.DsoClusterEvent;
import com.tc.cluster.DsoClusterListener;
import com.tc.cluster.DsoClusterTopology;
import com.tc.cluster.simulation.SimulatedDsoCluster;
import com.tc.injection.annotations.InjectedDsoInstance;
import com.tcclient.cluster.DsoNode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.modules.annotations.HonorTransient;
import org.terracotta.modules.annotations.InstrumentedClass;
import org.terracotta.modules.annotations.Root;
import terrastore.communication.Node;
import terrastore.cluster.Coordinator;
import terrastore.communication.ProcessingException;
import terrastore.ensemble.EnsembleConfiguration;
import terrastore.communication.Cluster;
import terrastore.router.MissingRouteException;
import terrastore.store.FlushCondition;
import terrastore.store.FlushStrategy;
import terrastore.communication.local.LocalNode;
import terrastore.communication.local.LocalProcessor;
import terrastore.communication.remote.RemoteProcessor;
import terrastore.communication.remote.RemoteNode;
import terrastore.ensemble.Discovery;
import terrastore.ensemble.impl.DefaultDiscovery;
import terrastore.ensemble.RemoteNodeFactory;
import terrastore.event.EventBus;
import terrastore.router.Router;
import terrastore.store.BackupManager;
import terrastore.store.SnapshotManager;
import terrastore.store.Store;
import terrastore.store.impl.TCStore;

/**
 * @author Sergio Bossa
 */
@InstrumentedClass
@HonorTransient
public class TCCoordinator implements Coordinator, DsoClusterListener {

    private static final Logger LOG = LoggerFactory.getLogger(TCCoordinator.class);
    //
    @Root
    private static final TCCoordinator INSTANCE = new TCCoordinator();
    //
    @InjectedDsoInstance
    private DsoCluster dsoCluster;
    //
    private Store store = new TCStore();
    private ReentrantLock stateLock = new ReentrantLock();
    private Map<String, Address> addressTable = new HashMap<String, Address>();
    private Condition setupAddressCondition = stateLock.newCondition();
    //
    private volatile transient Cluster thisCluster;
    private volatile transient String thisNodeName;
    private volatile transient String thisNodeHost;
    private volatile transient int thisNodePort;
    private volatile transient ConcurrentMap<String, Node> nodes;
    private volatile transient LocalProcessor localProcessor;
    private volatile transient RemoteProcessor remoteProcessor;
    //
    private volatile transient int maxFrameLength;
    private volatile transient long nodeTimeout;
    private volatile transient int localProcessorThreads;
    private volatile transient int remoteProcessorThreads;
    private volatile transient int globalExecutorThreads;
    //
    private volatile transient ExecutorService globalExecutor;
    //
    private volatile transient Router router;
    private volatile transient FlushStrategy flushStrategy;
    private volatile transient FlushCondition flushCondition;
    //
    private volatile transient Discovery ensembleDiscovery;
    //
    private volatile transient SnapshotManager snapshotManager;
    private volatile transient BackupManager backupManager;
    private volatile transient EventBus eventBus;

    private TCCoordinator() {
    }

    public static TCCoordinator getInstance() {
        return INSTANCE;
    }

    @Override
    public ExecutorService getGlobalExecutor() {
        return globalExecutor;
    }

    @Override
    public void setNodeTimeout(long nodeTimeout) {
        this.nodeTimeout = nodeTimeout;
    }

    @Override
    public void setWokerThreads(int workerThreads) {
        int threads = workerThreads / 3;
        this.localProcessorThreads = threads;
        this.remoteProcessorThreads = threads;
        this.globalExecutorThreads = threads;
    }

    @Override
    public void setMaxFrameLength(int maxFrameLength) {
        this.maxFrameLength = maxFrameLength;
    }

    @Override
    public void setRouter(Router router) {
        this.router = router;
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

    @Override
    public void setFlushStrategy(FlushStrategy flushStrategy) {
        this.flushStrategy = flushStrategy;
    }

    @Override
    public void setFlushCondition(FlushCondition flushCondition) {
        this.flushCondition = flushCondition;
    }

    public void start(String host, int port, EnsembleConfiguration ensembleConfiguration) {
        stateLock.lock();
        try {
            // Configure local data:
            thisCluster = new Cluster(ensembleConfiguration.getLocalCluster(), true);
            thisNodeName = getServerId(dsoCluster.getCurrentNode());
            thisNodeHost = host;
            thisNodePort = port;
            // Configure transients:
            nodes = new ConcurrentHashMap<String, Node>();
            globalExecutor = Executors.newFixedThreadPool(globalExecutorThreads);
            // Do manual injection on shared objects:
            store.setSnapshotManager(snapshotManager);
            store.setBackupManager(backupManager);
            store.setEventBus(eventBus);
            store.setTaskExecutor(globalExecutor);
            // Setup ensemble:
            setupEnsemble(ensembleConfiguration);
            // Add cluster listener to listen to events:
            getDsoCluster().addClusterListener(this);
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            stateLock.unlock();
        }
    }

    public void operationsEnabled(DsoClusterEvent event) {
    }

    public void nodeJoined(DsoClusterEvent event) {
        String joinedNodeName = getServerId(event.getNode());
        if (isThisNode(joinedNodeName)) {
            stateLock.lock();
            try {
                LOG.info("Joining this node {}", thisNodeName);
                setupThisNode();
                setupThisRemoteProcessor();
                setupAddressTable();
                setupRemoteNodes();
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
            } finally {
                stateLock.unlock();
            }
        } else {
            stateLock.lock();
            try {
                LOG.info("Joining remote node {}", joinedNodeName);
                connectRemoteNode(joinedNodeName);
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
            } finally {
                stateLock.unlock();
                pauseProcessing();
                flushThisNodeKeys();
                resumeProcessing();
            }
        }
    }

    public void nodeLeft(DsoClusterEvent event) {
        String leftNodeName = getServerId(event.getNode());
        if (!isThisNode(leftNodeName)) {
            stateLock.lock();
            try {
                disconnectRemoteNode(leftNodeName);
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
            } finally {
                stateLock.unlock();
                pauseProcessing();
                flushThisNodeKeys();
                resumeProcessing();
            }
        }
    }

    public void operationsDisabled(DsoClusterEvent event) {
        try {
            LOG.info("Disabling cluster node {}", thisNodeName);
            disconnectEverything();
            cleanupEverything();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            doExit();
        }
    }

    private DsoCluster getDsoCluster() {
        if (dsoCluster != null) {
            return dsoCluster;
        } else {
            return new SimulatedDsoCluster();
        }
    }

    private String getServerId(DsoNode dsoNode) {
        return dsoNode.getId().replace("Client", "Server");
    }

    private boolean isThisNode(String candidateNodeName) {
        return thisNodeName.equals(candidateNodeName);
    }

    private void setupThisRemoteProcessor() {
        remoteProcessor = new RemoteProcessor(thisNodeHost, thisNodePort, maxFrameLength, remoteProcessorThreads, router);
        remoteProcessor.start();
        LOG.info("Set up processor for {}", thisNodeName);
    }

    private void setupThisNode() {
        localProcessor = new LocalProcessor(localProcessorThreads, store);
        LocalNode thisNode = new LocalNode(thisNodeHost, thisNodePort, thisNodeName, localProcessor);
        localProcessor.start();
        thisNode.connect();
        nodes.put(thisNodeName, thisNode);
        router.addRouteToLocalNode(thisNode);
        router.addRouteTo(thisCluster, thisNode);
        LOG.info("Set up this node {}", thisNodeName);
    }

    private void setupAddressTable() {
        addressTable.put(thisNodeName, new Address(thisNodeHost, thisNodePort));
        setupAddressCondition.signalAll();
    }

    private void setupRemoteNodes() throws InterruptedException {
        DsoClusterTopology dsoTopology = getDsoCluster().getClusterTopology();
        for (DsoNode dsoNode : dsoTopology.getNodes()) {
            String serverId = getServerId(dsoNode);
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
        Address remoteNodeAddress = addressTable.get(remoteNodeName);
        if (remoteNodeAddress != null) {
            // Double check to tolerate duplicated node joins by terracotta server:
            if (!nodes.containsKey(remoteNodeName)) {
                Node remoteNode = new RemoteNode(
                        remoteNodeAddress.getHost(),
                        remoteNodeAddress.getPort(),
                        remoteNodeName,
                        maxFrameLength,
                        nodeTimeout);
                remoteNode.connect();
                nodes.put(remoteNodeName, remoteNode);
                router.addRouteTo(thisCluster, remoteNode);
                LOG.info("Set up remote node {}", remoteNodeName);
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
        LOG.info("About to flush keys on node {}", thisNodeName);
        store.flush(flushStrategy, flushCondition);
    }

    private void disconnectRemoteNode(String nodeName) {
        Node remoteNode = nodes.remove(nodeName);
        remoteNode.disconnect();
        router.removeRouteTo(thisCluster, remoteNode);
        LOG.info("Discarded node {}", nodeName);
    }

    private void disconnectEverything() {
        for (Node node : nodes.values()) {
            node.disconnect();
        }
        localProcessor.stop();
        remoteProcessor.stop();
    }

    private void cleanupEverything() {
        nodes.clear();
        router.cleanup();
        eventBus.shutdown();
        globalExecutor.shutdownNow();
    }

    private void doExit() {
        new Thread() {

            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (Exception ex) {
                } finally {
                    LOG.info("Exiting cluster node {}", thisNodeName);
                    System.exit(0);
                }
            }
        }.start();
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
        ensembleDiscovery = new DefaultDiscovery(router, new RemoteNodeFactory() {

            @Override
            public Node makeNode(String host, int port, String name) {
                return new RemoteNode(thisNodeHost, thisNodePort, thisNodeName, maxFrameLength, nodeTimeout);
            }
        });
        for (Map.Entry<String, String> entry : ensembleConfiguration.getSeeds().entrySet()) {
            String cluster = entry.getKey();
            String seed = entry.getValue();
            LOG.info("Joining remote cluster {}", cluster);
            ensembleDiscovery.join(clusters.get(cluster), seed);
        }
        ensembleDiscovery.schedule(ensembleConfiguration.getDiscoveryInterval(), ensembleConfiguration.getDiscoveryInterval(), TimeUnit.MILLISECONDS);
    }

    @InstrumentedClass
    private static class Address {

        private String host;
        private int port;

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
