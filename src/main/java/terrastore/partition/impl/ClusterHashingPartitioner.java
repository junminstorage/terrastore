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
package terrastore.partition.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.router.impl.HashFunction;
import terrastore.partition.ClusterPartitioner;
import terrastore.store.Key;

/**
 * {@link terrastore.partition.ClusterPartitioner} implementation based on consistent hashing and ordering.
 *
 * @author Sergio Bossa
 */
public class ClusterHashingPartitioner implements ClusterPartitioner {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterHashingPartitioner.class);
    //
    private final int maxPartitions;
    private final HashFunction hashFunction;
    private final Map<Cluster, Partitioner> partitioners;
    private final ReadWriteLock stateLock;

    public ClusterHashingPartitioner(int maxPartitions, HashFunction hashFunction) {
        this.maxPartitions = maxPartitions;
        this.hashFunction = hashFunction;
        this.partitioners = new HashMap<Cluster, Partitioner>();
        this.stateLock = new ReentrantReadWriteLock();
    }

    @Override
    public int getMaxPartitions() {
        return maxPartitions;
    }

    @Override
    public void addNode(Cluster cluster, Node node) {
        stateLock.writeLock().lock();
        try {
            Partitioner partitioner = partitioners.get(cluster);
            if (partitioner == null) {
                partitioner = new Partitioner(maxPartitions, hashFunction);
                partitioners.put(cluster, partitioner);
            }
            partitioner.addNode(node);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public void removeNode(Cluster cluster, Node node) {
        stateLock.writeLock().lock();
        try {
            Partitioner partitioner = partitioners.get(cluster);
            if (partitioner != null) {
                partitioner.removeNode(node);
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Set<Node> getNodesFor(Cluster cluster) {
        stateLock.readLock().lock();
        try {
            Partitioner partitioner = partitioners.get(cluster);
            if (partitioner != null) {
                return partitioner.getNodes();
            } else {
                return Collections.emptySet();
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Node getNodeFor(Cluster cluster, String bucket) {
        stateLock.readLock().lock();
        try {
            Partitioner partitioner = partitioners.get(cluster);
            if (partitioner != null) {
                return partitioner.getNodeFor(bucket);
            } else {
                return null;
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Node getNodeFor(Cluster cluster, String bucket, Key key) {
        stateLock.readLock().lock();
        try {
            Partitioner partitioner = partitioners.get(cluster);
            if (partitioner != null) {
                return partitioner.getNodeFor(bucket, key);
            } else {
                return null;
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void cleanupPartitions() {
        stateLock.writeLock().lock();
        try {
            for (Partitioner partitioner : partitioners.values()) {
                partitioner.cleanupPartitions();
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    private static class Partitioner {

        private final int maxPartitions;
        private final HashFunction hashFunction;
        private final Node[] ring;
        private final SortedSet<Node> nodes;
        private final Map<Node, List<Integer>> nodesToPartitions;

        public Partitioner(int maxPartitions, HashFunction hashFunction) {
            this.maxPartitions = maxPartitions;
            this.hashFunction = hashFunction;
            this.ring = new Node[maxPartitions];
            this.nodes = new TreeSet<Node>(new NodeComparator());
            this.nodesToPartitions = new HashMap();
        }

        public void addNode(Node node) {
            if (nodes.size() == maxPartitions) {
                // TODO : use proper exception here!
                throw new IllegalStateException("Reached partitions limit: " + maxPartitions);
            } else if (!nodes.contains(node)) {
                nodes.add(node);
                rebuildPartitions();
            } else {
                // TODO : use proper exception here?
                throw new IllegalStateException("Duplicated node: " + node.getName());
            }
        }

        public void removeNode(Node node) {
            if (nodes.contains(node)) {
                nodes.remove(node);
                rebuildPartitions();
            } else {
                // TODO : use proper exception here?
                throw new IllegalStateException("Not existent node: " + node.getName());
            }
        }

        public Set<Node> getNodes() {
            return nodes;
        }

        public Node getNodeFor(String bucket) {
            String toHash = bucket;
            int hash = hashFunction.hash(toHash, maxPartitions);
            return selectNodeAtPartition(hash);
        }

        public Node getNodeFor(String bucket, Key key) {
            String toHash = bucket + key;
            int hash = hashFunction.hash(toHash, maxPartitions);
            return selectNodeAtPartition(hash);
        }

        public void cleanupPartitions() {
            nodes.clear();
            nodesToPartitions.clear();
        }

        private void rebuildPartitions() {
            int totalNodes = nodes.size();
            if (totalNodes > 0) {
                int optimalPartitionSize = maxPartitions / totalNodes;
                int ringPosition = 0;
                int nodePosition = 1;
                nodesToPartitions.clear();
                for (Node node : nodes) {
                    List<Integer> nodePartitions = new ArrayList<Integer>(optimalPartitionSize + (maxPartitions % totalNodes));
                    for (int i = 0; (i < optimalPartitionSize) || (nodePosition == totalNodes && ringPosition < ring.length); i++) {
                        nodePartitions.add(ringPosition);
                        ring[ringPosition] = node;
                        ringPosition++;
                    }
                    nodesToPartitions.put(node, nodePartitions);
                    nodePosition++;
                }
            } else {
                for (int i = 0; i < maxPartitions; i++) {
                    ring[i] = null;
                }
                nodesToPartitions.clear();
            }
        }

        private Node selectNodeAtPartition(int partition) {
            if (partition >= 0 && partition < ring.length) {
                Node selected = ring[partition];
                LOG.debug("Getting node {} at partition {}", selected, partition);
                return selected;
            } else {
                // TODO : use proper exception here?
                throw new IllegalStateException("Wrong partition number: " + partition);
            }
        }
    }

    private static class NodeComparator implements Comparator<Node> {

        public int compare(Node n1, Node n2) {
            return n1.getName().compareTo(n2.getName());
        }
    }
}
