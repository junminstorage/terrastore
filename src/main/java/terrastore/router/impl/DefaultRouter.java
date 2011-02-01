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
package terrastore.router.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.partition.ClusterPartitioner;
import terrastore.partition.EnsemblePartitioner;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Key;
import terrastore.util.collect.Sets;

/**
 * Default {@link terrastore.router.Router} implementation.
 *
 * @author Sergio Bossa
 */
public class DefaultRouter implements Router {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRouter.class);
    //
    private final ReadWriteLock stateLock;
    private final Set<Cluster> clustersCache;
    private final ClusterPartitioner clusterPartitioner;
    private final EnsemblePartitioner ensemblePartitioner;
    private volatile Node localNode;

    public DefaultRouter(ClusterPartitioner clusterPartitioner, EnsemblePartitioner ensemblePartitioner) {
        this.stateLock = new ReentrantReadWriteLock();
        this.clustersCache = new HashSet<Cluster>();
        this.clusterPartitioner = clusterPartitioner;
        this.ensemblePartitioner = ensemblePartitioner;
    }

    @Override
    public void setupClusters(Set<Cluster> clusters) {
        stateLock.writeLock().lock();
        try {
            clustersCache.addAll(clusters);
            ensemblePartitioner.setupClusters(clusters);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Set<Cluster> getClusters() {
        return Collections.unmodifiableSet(clustersCache);
    }

    @Override
    public void addRouteToLocalNode(Node node) {
        localNode = node;
    }

    @Override
    public void addRouteTo(Cluster cluster, Node node) {
        stateLock.writeLock().lock();
        try {
            if (clustersCache.contains(cluster)) {
                LOG.debug("Adding route to cluster {} and node {}", cluster, node);
                clusterPartitioner.addNode(cluster, node);
            } else {
                // TODO : use proper exception here?
                throw new IllegalStateException("Unknown cluster: " + cluster.getName());
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public void removeRouteTo(Cluster cluster, Node node) {
        stateLock.writeLock().lock();
        try {
            if (clustersCache.contains(cluster)) {
                LOG.debug("Removing route to cluster {} and node {}", cluster, node);
                clusterPartitioner.removeNode(cluster, node);
            } else {
                // TODO : use proper exception here?
                throw new IllegalStateException("Unknown cluster: " + cluster.getName());
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Node routeToLocalNode() {
        LOG.debug("Routing to local node {}", localNode);
        return localNode;
    }

    @Override
    public Node routeToNodeFor(String bucket) throws MissingRouteException {
        stateLock.readLock().lock();
        try {
            Cluster cluster = ensemblePartitioner.getClusterFor(bucket);
            if (cluster != null) {
                Node route = clusterPartitioner.getNodeFor(cluster, bucket);
                if (route != null) {
                    LOG.debug("Routing to cluster {} and node {}", cluster, route);
                    return route;
                } else {
                    throw new MissingRouteException(new ErrorMessage(ErrorMessage.UNAVAILABLE_ERROR_CODE, "Data is currently unavailable. Some clusters of your ensemble may be down or unreachable."));
                }
            } else {
                // TODO : use proper exception here?
                throw new IllegalStateException("Cannot find cluster for bucket " + bucket);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Node routeToNodeFor(String bucket, Key key) throws MissingRouteException {
        stateLock.readLock().lock();
        try {
            Cluster cluster = ensemblePartitioner.getClusterFor(bucket, key);
            if (cluster != null) {
                Node route = clusterPartitioner.getNodeFor(cluster, bucket, key);
                if (route != null) {
                    LOG.debug("Routing to cluster {} and node {}", cluster, route);
                    return route;
                } else {
                    throw new MissingRouteException(new ErrorMessage(ErrorMessage.UNAVAILABLE_ERROR_CODE, "Data is currently unavailable. Some clusters of your ensemble may be down or unreachable."));
                }
            } else {
                // TODO : use proper exception here?
                throw new IllegalStateException("Cannot find cluster for bucket " + bucket + " and key " + key);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Map<Node, Set<Key>> routeToNodesFor(String bucket, Set<Key> keys) throws MissingRouteException {
        stateLock.readLock().lock();
        try {
            Map<Node, Set<Key>> nodeToKeys = new HashMap<Node, Set<Key>>();
            for (Key key : keys) {
                Node route = routeToNodeFor(bucket, key);
                Set<Key> routedKeys = nodeToKeys.get(route);
                if (routedKeys == null) {
                    routedKeys = new HashSet<Key>();
                    nodeToKeys.put(route, routedKeys);
                }
                routedKeys.add(key);
            }
            return nodeToKeys;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Set<Node> clusterRoute(Cluster cluster) {
        stateLock.readLock().lock();
        try {
            LOG.debug("Routing to all nodes for cluster {}", cluster);
            if (cluster.isLocal()) {
                return Sets.cons(localNode, clusterPartitioner.getNodesFor(cluster));
            } else {
                return clusterPartitioner.getNodesFor(cluster);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Map<Cluster, Set<Node>> broadcastRoute() {
        stateLock.readLock().lock();
        try {
            LOG.debug("Routing to all nodes of all clusters.");
            Map<Cluster, Set<Node>> nodes = new HashMap<Cluster, Set<Node>>(clustersCache.size());
            for (Cluster cluster : clustersCache) {
                if (cluster.isLocal()) {
                    nodes.put(cluster, Sets.cons(localNode, clusterPartitioner.getNodesFor(cluster)));
                } else {
                    nodes.put(cluster, clusterPartitioner.getNodesFor(cluster));
                }
            }
            return nodes;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void cleanup() {
        stateLock.writeLock().lock();
        try {
            clusterPartitioner.cleanupPartitions();
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public ClusterPartitioner getClusterPartitioner() {
        return clusterPartitioner;
    }

    @Override
    public EnsemblePartitioner getEnsemblePartitioner() {
        return ensemblePartitioner;
    }
}
