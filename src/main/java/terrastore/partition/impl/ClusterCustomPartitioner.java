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
package terrastore.partition.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.partition.ClusterPartitioner;
import terrastore.partition.CustomClusterPartitionerStrategy;
import terrastore.store.Key;

/**
 * {@link terrastore.partition.ClusterPartitioner} implementation delegating to
 * {@link ClusterPartitionerStrategy} for actual partitioning.
 *
 * @author Sergio Bossa
 */
public class ClusterCustomPartitioner implements ClusterPartitioner {

    private final ReadWriteLock stateLock;
    private final CustomClusterPartitionerStrategy strategy;
    private final Map<String, Node> allNodes;
    private final Map<Cluster, Set<Node>> perClusterNodes;

    public ClusterCustomPartitioner(CustomClusterPartitionerStrategy strategy) {
        this.strategy = strategy;
        this.stateLock = new ReentrantReadWriteLock();
        this.allNodes = new HashMap<String, Node>();
        this.perClusterNodes = new HashMap<Cluster, Set<Node>>();
    }

    @Override
    public int getMaxPartitions() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void addNode(Cluster cluster, Node node) {
        stateLock.writeLock().lock();
        try {
            Set<Node> nodes = perClusterNodes.get(cluster);
            if (nodes == null) {
                nodes = new HashSet<Node>();
                perClusterNodes.put(cluster, nodes);
            }
            nodes.add(node);
            allNodes.put(keyFor(cluster.getName(), node.getHost(), node.getPort()), node);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public void removeNode(Cluster cluster, Node node) {
        stateLock.writeLock().lock();
        try {
            Set<Node> nodes = perClusterNodes.get(cluster);
            if (nodes != null) {
                nodes.remove(node);
            }
            allNodes.remove(keyFor(cluster.getName(), node.getHost(), node.getPort()));
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Set<Node> getNodesFor(Cluster cluster) {
        stateLock.readLock().lock();
        try {
            Set<Node> nodes = perClusterNodes.get(cluster);
            if (nodes != null) {
                return nodes;
            } else {
                return Collections.EMPTY_SET;
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Node getNodeFor(Cluster cluster, String bucket) {
        stateLock.readLock().lock();
        try {
            CustomClusterPartitionerStrategy.Node node = strategy.getNodeFor(cluster.getName(), bucket);
            if (node != null) {
                return allNodes.get(keyFor(cluster.getName(), node.getHost(), node.getPort()));
            } else {
                throw new IllegalStateException("Null partition for bucket " + bucket);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Node getNodeFor(Cluster cluster, String bucket, Key key) {
        stateLock.readLock().lock();
        try {
            CustomClusterPartitionerStrategy.Node node = strategy.getNodeFor(cluster.getName(), bucket, key.toString());
            if (node != null) {
                return allNodes.get(keyFor(cluster.getName(), node.getHost(), node.getPort()));
            } else {
                throw new IllegalStateException("Null partition for bucket " + bucket + " and key " + key);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void cleanupPartitions() {
        stateLock.writeLock().lock();
        try {
            perClusterNodes.clear();
            allNodes.clear();
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    private String keyFor(String cluster, String host, int port) {
        return cluster + ":" + host + ":" + port;
    }
}
