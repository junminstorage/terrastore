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

/**
 * {@link terrastore.router.Router} implementation.
 *
 * @author Sergio Bossa
 */
public class DefaultRouter implements Router {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRouter.class);
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
    public void addRouteToLocalNode(Node node) {
        localNode = node;
    }

    @Override
    public void addRouteTo(Cluster cluster, Node node) {
        stateLock.writeLock().lock();
        try {
            if (clustersCache.contains(cluster)) {
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
        return localNode;
    }

    @Override
    public Node routeToNodeFor(String bucket) throws MissingRouteException {
        stateLock.readLock().lock();
        try {
            Cluster cluster = ensemblePartitioner.getClusterFor(bucket);
            Node route = clusterPartitioner.getNodeFor(cluster, bucket);
            if (route != null) {
                LOG.info("Routing to cluster {} with node {}", cluster, route);
                return route;
            } else {
                throw new MissingRouteException(new ErrorMessage(ErrorMessage.UNAVAILABLE_ERROR_CODE, "Data is currently unavailable"));
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Node routeToNodeFor(String bucket, String key) throws MissingRouteException {
        stateLock.readLock().lock();
        try {
            Cluster cluster = ensemblePartitioner.getClusterFor(bucket);
            Node route = clusterPartitioner.getNodeFor(cluster, bucket, key);
            if (route != null) {
                LOG.info("Routing to cluster {} with node {}", cluster, route);
                return route;
            } else {
                throw new MissingRouteException(new ErrorMessage(ErrorMessage.UNAVAILABLE_ERROR_CODE, "Data is currently unavailable"));
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Map<Node, Set<String>> routeToNodesFor(String bucket, Set<String> keys) throws MissingRouteException {
        stateLock.readLock().lock();
        try {
            Map<Node, Set<String>> nodeToKeys = new HashMap<Node, Set<String>>();
            for (String key : keys) {
                Node route = routeToNodeFor(bucket, key);
                Set<String> routedKeys = nodeToKeys.get(route);
                if (routedKeys == null) {
                    routedKeys = new HashSet<String>();
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
    public Set<Node> broadcastRoute() throws MissingRouteException {
        stateLock.readLock().lock();
        try {
            Set<Node> nodes = new HashSet<Node>(clustersCache.size());
            for (Cluster cluster : clustersCache) {
                Node route = clusterPartitioner.getNodeFor(cluster);
                if (route != null) {
                    nodes.add(route);
                } else {
                    throw new MissingRouteException(new ErrorMessage(ErrorMessage.UNAVAILABLE_ERROR_CODE, "Data is currently unavailable"));
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
