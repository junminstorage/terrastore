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
package terrastore.cluster.ensemble.impl;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.communication.NodeConfiguration;
import terrastore.common.ErrorMessage;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.RemoteNodeFactory;
import terrastore.cluster.ensemble.impl.View.Member;
import terrastore.communication.protocol.MembershipCommand;
import terrastore.cluster.ensemble.EnsembleManager;
import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.cluster.ensemble.EnsembleScheduler;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;

/**
 * Default {@link terrastore.ensemble.EnsembleManager} implementation.
 *
 * @author Sergio Bossa
 */
public class DefaultEnsembleManager implements EnsembleManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultEnsembleManager.class);
    //
    private final Map<String, EnsembleScheduler> ensembleSchedulers;
    private final Router router;
    private final RemoteNodeFactory remoteNodeFactory;
    //
    private final ConcurrentMap<Cluster, Node> bootstrapNodes;
    private final ConcurrentMap<Cluster, List<Node>> perClusterNodes;
    private final ConcurrentMap<Cluster, View> perClusterViews;
    //
    private EnsembleConfiguration configuration;

    public DefaultEnsembleManager(Map<String, EnsembleScheduler> ensembleSchedulers, Router router, RemoteNodeFactory nodeFactory) {
        this.ensembleSchedulers = ensembleSchedulers;
        this.router = router;
        this.remoteNodeFactory = nodeFactory;
        this.bootstrapNodes = new ConcurrentHashMap<Cluster, Node>();
        this.perClusterNodes = new ConcurrentHashMap<Cluster, List<Node>>();
        this.perClusterViews = new ConcurrentHashMap<Cluster, View>();
    }

    @Override
    public final synchronized void join(Cluster cluster, String seed, EnsembleConfiguration ensembleConfiguration) throws MissingRouteException, ProcessingException {
        EnsembleScheduler scheduler = ensembleSchedulers.get(ensembleConfiguration.getDiscovery().getType());
        this.configuration = ensembleConfiguration;
        if (scheduler != null) {
            if (!cluster.isLocal()) {
                String[] hostPortPair = seed.split(":");
                bootstrapNodes.put(cluster, remoteNodeFactory.makeRemoteNode(new NodeConfiguration(seed, hostPortPair[0], Integer.parseInt(hostPortPair[1]), "", 0)));
                scheduler.schedule(cluster, this, configuration);
            } else {
                throw new IllegalArgumentException("No need to join local cluster: " + cluster);
            }
        } else {
            throw new IllegalArgumentException("No ensemble scheduler of type: " + configuration.getDiscovery().getType());
        }
    }

    @Override
    public synchronized final View update(Cluster cluster) throws MissingRouteException, ProcessingException {
        View view = null;
        try {
            List<Node> nodes = perClusterNodes.get(cluster);
            if (nodes == null || nodes.isEmpty()) {
                LOG.debug("Bootstrapping discovery for cluster {}", cluster);
                Node bootstrap = bootstrapNodes.get(cluster);
                if (bootstrap != null) {
                    try {
                        bootstrap.connect();
                        view = requestMembership(cluster, Arrays.asList(bootstrap));
                        calculateView(cluster, view);
                    } finally {
                        bootstrap.disconnect();
                    }
                }
            } else {
                LOG.debug("Updating cluster view for {}", cluster);
                view = requestMembership(cluster, nodes);
                calculateView(cluster, view);
            }
        } catch (Exception ex) {
            LOG.warn("Error updating membership information for cluster {}", cluster);
            LOG.debug(ex.getMessage(), ex);
            clearView(cluster);
        }
        return view;
    }

    @Override
    public synchronized void shutdown() {
        cancelScheduler();
        disconnectAllClustersNodes();
    }

    @Override
    public Map<String, EnsembleScheduler> getEnsembleSchedulers() {
        return Collections.unmodifiableMap(ensembleSchedulers);
    }

    @Override
    public Router getRouter() {
        return router;
    }

    @Override
    public RemoteNodeFactory getRemoteNodeFactory() {
        return remoteNodeFactory;
    }

    private void cancelScheduler() {
        if (configuration != null) {
            EnsembleScheduler scheduler = ensembleSchedulers.get(configuration.getDiscovery().getType());
            if (scheduler != null) {
                scheduler.shutdown();
            } else {
                throw new IllegalArgumentException("No ensmeble scheduler of type: " + configuration.getDiscovery().getType());
            }
        }
    }

    private View requestMembership(Cluster cluster, List<Node> contactNodes) throws MissingRouteException, ProcessingException {
        Iterator<Node> nodeIterator = contactNodes.iterator();
        boolean successful = false;
        View view = null;
        while (!successful && nodeIterator.hasNext()) {
            Node node = null;
            try {
                node = nodeIterator.next();
                view = node.<View>send(new MembershipCommand());
                successful = true;
                LOG.debug("Updated cluster view from node {}:{}", cluster, node);
            } catch (Exception ex) {
                LOG.warn("Failed to contact ensemble node {}:{} for updating cluster view!", cluster, node);
                router.removeRouteTo(cluster, node);
                node.disconnect();
                nodeIterator.remove();
                LOG.info("Disconnected ensemble remote node {}:{}", cluster, node);
            }
        }
        if (successful) {
            return view;
        } else {
            throw new MissingRouteException(new ErrorMessage(ErrorMessage.UNAVAILABLE_ERROR_CODE, "No route to cluster " + cluster));
        }
    }

    private void calculateView(Cluster cluster, View updatedView) {
        List<Node> currentNodes = perClusterNodes.get(cluster);
        if (currentNodes == null) {
            currentNodes = new LinkedList<Node>();
            perClusterNodes.put(cluster, currentNodes);
        }
        //
        View currentView = perClusterViews.get(cluster);
        if (currentView != null) {
            LOG.debug("Current view for cluster {} : {}", cluster, currentView);
            LOG.debug("Updated view for cluster {} : {}", cluster, updatedView);
            Set<View.Member> leavingMembers = Sets.difference(currentView.getMembers(), updatedView.getMembers());
            Set<View.Member> joiningMembers = Sets.difference(updatedView.getMembers(), currentView.getMembers());
            for (View.Member member : leavingMembers) {
                Node node = findNode(currentNodes, member);
                if (node != null) {
                    router.removeRouteTo(cluster, node);
                    node.disconnect();
                    currentNodes.remove(node);
                    LOG.info("Disconnected ensemble remote node {}:{}", cluster, node);
                }
            }
            for (View.Member member : joiningMembers) {
                Node node = remoteNodeFactory.makeRemoteNode(member.getConfiguration());
                router.addRouteTo(cluster, node);
                node.connect();
                currentNodes.add(node);
                LOG.info("Joining ensemble remote node {}:{}", cluster, node);
            }
        } else {
            LOG.debug("No current view for cluster {}", cluster);
            LOG.debug("Updated view for cluster {} :  {}", cluster, updatedView);
            for (View.Member member : updatedView.getMembers()) {
                Node node = remoteNodeFactory.makeRemoteNode(member.getConfiguration());
                router.addRouteTo(cluster, node);
                node.connect();
                currentNodes.add(node);
                LOG.info("Joining ensemble remote node {}:{}", cluster, node);
            }
        }
        perClusterViews.put(cluster, updatedView);
    }

    private void clearView(Cluster cluster) {
        perClusterViews.remove(cluster);
    }

    private Node findNode(List<Node> nodes, Member member) {
        try {
            return Iterables.find(nodes, new NodeFinder(member.getConfiguration().getName()));
        } catch (NoSuchElementException ex) {
            return null;
        }
    }

    private void disconnectAllClustersNodes() {
        for (List<Node> nodes : perClusterNodes.values()) {
            for (Node node : nodes) {
                try {
                    node.disconnect();
                } catch (Exception ex) {
                }
            }
        }
    }

    private static class NodeFinder implements Predicate<Node> {

        private final String name;

        public NodeFinder(String name) {
            this.name = name;
        }

        @Override
        public boolean apply(Node node) {
            return node.getName().equals(name);
        }

    }
}
