/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.modules.annotations.HonorTransient;
import org.terracotta.modules.annotations.InstrumentedClass;
import org.terracotta.modules.annotations.Root;
import terrastore.communication.Node;
import terrastore.cluster.Cluster;
import terrastore.cluster.FlushCondition;
import terrastore.cluster.FlushStrategy;
import terrastore.communication.local.LocalNode;
import terrastore.communication.remote.RemoteProcessor;
import terrastore.communication.remote.RemoteNode;
import terrastore.communication.remote.pipe.Topology;
import terrastore.communication.protocol.Command;
import terrastore.communication.protocol.Response;
import terrastore.communication.serialization.JavaSerializer;
import terrastore.router.Router;
import terrastore.store.Store;
import terrastore.store.impl.TCStore;

/**
 * @author Sergio Bossa
 */
@InstrumentedClass
@HonorTransient
public class TCCluster implements Cluster, DsoClusterListener {

    private static final Logger LOG = LoggerFactory.getLogger(TCCluster.class);
    //
    @Root
    private static final TCCluster INSTANCE = new TCCluster();
    //
    @InjectedDsoInstance
    private DsoCluster dsoCluster;
    //
    // TODO: cleanup unused pipes
    private Topology pipes = new Topology(new JavaSerializer<Command>(), new JavaSerializer<Response>());
    private Store store = new TCStore();
    private ReentrantLock stateLock = new ReentrantLock();
    //
    private volatile transient Map<String, Node> nodes;
    private volatile transient RemoteProcessor processor;
    //
    private volatile transient long nodeTimeout;
    private volatile transient int workerThreads;
    private volatile transient ExecutorService workerExecutor;
    //
    private volatile transient Router router;
    private volatile transient FlushStrategy flushStrategy;
    private volatile transient FlushCondition flushCondition;

    private TCCluster() {
    }

    public static TCCluster getInstance() {
        return INSTANCE;
    }

    @Override
    public void setNodeTimeout(long nodeTimeout) {
        this.nodeTimeout = nodeTimeout;
    }

    @Override
    public void setWokerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public ExecutorService getWorkerExecutor() {
        return workerExecutor;
    }

    public Router getRouter() {
        return router;
    }

    public FlushStrategy getFlushStrategy() {
        return flushStrategy;
    }

    public FlushCondition getFlushCondition() {
        return flushCondition;
    }

    public void setRouter(Router router) {
        this.router = router;
    }

    public void setFlushStrategy(FlushStrategy flushStrategy) {
        this.flushStrategy = flushStrategy;
    }

    public void setFlushCondition(FlushCondition flushCondition) {
        this.flushCondition = flushCondition;
    }

    public void start() {
        String thisNodeName = dsoCluster.getCurrentNode().getId();
        stateLock.lock();
        try {
            LOG.info("Setting up cluster node {}", thisNodeName);
            workerExecutor = Executors.newFixedThreadPool(workerThreads);
            getDsoCluster().addClusterListener(this);
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void stop() {
        String thisNodeName = dsoCluster.getCurrentNode().getId();
        stateLock.lock();
        try {
            LOG.info("Stopping cluster node {}", thisNodeName);
            getDsoCluster().removeClusterListener(this);
            workerExecutor.shutdown();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            stateLock.unlock();
        }
    }

    public void operationsEnabled(DsoClusterEvent event) {
        String thisNodeName = dsoCluster.getCurrentNode().getId();
        stateLock.lock();
        try {
            nodes = new HashMap<String, Node>();
            setupThisNode(thisNodeName);
            setupRemoteProcessor(thisNodeName);
            setupRemoteNodes();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            stateLock.unlock();
        }
    }

    public void operationsDisabled(DsoClusterEvent event) {
        stateLock.lock();
        try {
            disconnectEverything();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            stateLock.unlock();
        }
    }

    public void nodeJoined(DsoClusterEvent event) {
        String joinedNodeName = event.getNode().getId();
        stateLock.lock();
        try {
            if (!isThisNode(joinedNodeName)) {
                setupRemoteNode(joinedNodeName, true);
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            stateLock.unlock();
        }
    }

    public void nodeLeft(DsoClusterEvent event) {
        String leftNodeKey = event.getNode().getId();
        stateLock.lock();
        try {
            discardRemoteNode(leftNodeKey);
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            stateLock.unlock();
        }
    }

    private DsoCluster getDsoCluster() {
        if (dsoCluster != null) {
            return dsoCluster;
        } else {
            return new SimulatedDsoCluster();
        }
    }

    private boolean isThisNode(String candidateNodeName) {
        return (dsoCluster.getCurrentNode().getId().equals(candidateNodeName));
    }

    private void setupRemoteProcessor(String thisNodeName) {
        processor = new RemoteProcessor(thisNodeName, pipes, store, workerExecutor);
        processor.start();
        LOG.info("Set up processor for {}", thisNodeName);
    }

    private void setupThisNode(String thisNodeName) {
        Node thisNode = new LocalNode(thisNodeName, store);
        thisNode.connect();
        nodes.put(thisNodeName, thisNode);
        router.setLocalNode(thisNode);
        router.addRouteTo(thisNode);
        LOG.info("Set up this node {}", thisNodeName);
    }

    private void setupRemoteNode(String remoteNodeName, boolean flush) {
        Node remoteNode = new RemoteNode(remoteNodeName, pipes, nodeTimeout);
        remoteNode.connect();
        nodes.put(remoteNodeName, remoteNode);
        router.addRouteTo(remoteNode);
        if (flush) {
            flushStrategy.flush(store, flushCondition);
        }
        LOG.info("Set up remote node {}", remoteNodeName);
    }

    private void setupRemoteNodes() {
        DsoNode currentNode = getDsoCluster().getCurrentNode();
        DsoClusterTopology dsoTopology = getDsoCluster().getClusterTopology();
        for (DsoNode dsoNode : dsoTopology.getNodes()) {
            if (!dsoNode.getId().equals(currentNode.getId())) {
                String remoteNodeName = dsoNode.getId();
                setupRemoteNode(remoteNodeName, false);
            }
        }
    }

    private void discardRemoteNode(String nodeName) {
        Node remoteNode = nodes.remove(nodeName);
        remoteNode.disconnect();
        router.removeRouteTo(remoteNode);
        LOG.info("Discarded node {}", nodeName);
    }

    private void disconnectEverything() {
        processor.stop();
        for (Node node : nodes.values()) {
            node.disconnect();
        }
        nodes.clear();
        router.cleanup();
        LOG.info("Disconnected everything.");
    }
}
