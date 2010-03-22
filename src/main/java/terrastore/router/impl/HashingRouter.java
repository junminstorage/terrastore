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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.ensemble.EnsembleManager;
import terrastore.partition.PartitionManager;
import terrastore.router.Router;

/**
 * {@link terrastore.router.Router} implementation using an hash function for associating buckets and keys to nodes
 * managed by the {@link terrastore.partition.PartitionManager}.
 *
 * @author Sergio Bossa
 */
public class HashingRouter implements Router {

    private static final Logger LOG = LoggerFactory.getLogger(HashingRouter.class);
    private final ReadWriteLock stateLock;
    private final HashFunction hashFunction;
    private final EnsembleManager ensembleManager;
    private final PartitionManager partitionManager;
    private Node localNode;

    public HashingRouter(HashFunction hashFunction, EnsembleManager ensembleManager, PartitionManager partitionManager) {
        this.stateLock = new ReentrantReadWriteLock();
        this.hashFunction = hashFunction;
        this.ensembleManager = ensembleManager;
        this.partitionManager = partitionManager;
    }

    @Override
    public void setupClusters(Set<Cluster> clusters) {
        stateLock.writeLock().lock();
        try {
            ensembleManager.setupClusters(clusters);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public void setLocalNode(Node node) {
        stateLock.writeLock().lock();
        try {
            localNode = node;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Node getLocalNode() {
        stateLock.writeLock().lock();
        try {
            return localNode;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public void addRouteTo(Cluster cluster, Node node) {
        stateLock.writeLock().lock();
        try {
            if (cluster.isLocal()) {
                partitionManager.addNode(node);
            } else {
                ensembleManager.addContactNode(cluster, node);
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public void removeRouteTo(Cluster cluster, Node node) {
        stateLock.writeLock().lock();
        try {
            if (cluster.isLocal()) {
                partitionManager.removeNode(node);
            } else {
                ensembleManager.removeContactNode(cluster, node);
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Node routeToNodeFor(String bucket) {
        stateLock.readLock().lock();
        try {
            Cluster cluster = ensembleManager.getClusterFor(bucket);
            if (cluster.isLocal()) {
                String toHash = bucket;
                int partitions = partitionManager.getMaxPartitions();
                int hash = hashFunction.hash(toHash, partitions);
                Node route = partitionManager.selectNodeAtPartition(hash);
                LOG.info("Routing to: {}", route);
                return route;
            } else {
                return ensembleManager.getContactNodeFor(cluster);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Node routeToNodeFor(String bucket, String key) {
        stateLock.readLock().lock();
        try {
            Cluster cluster = ensembleManager.getClusterFor(bucket, key);
            if (cluster.isLocal()) {
                String toHash = bucket + key;
                int partitions = partitionManager.getMaxPartitions();
                int hash = hashFunction.hash(toHash, partitions);
                Node route = partitionManager.selectNodeAtPartition(hash);
                LOG.info("Routing to: {}", route);
                return route;
            } else {
                return ensembleManager.getContactNodeFor(cluster);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Map<Node, Set<String>> routeToNodesFor(String bucket, Set<String> keys) {
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
    public void cleanup() {
        stateLock.writeLock().lock();
        try {
            partitionManager.cleanupPartitions();
            localNode = null;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    @Override
    public EnsembleManager getEnsembleManager() {
        return ensembleManager;
    }
}
