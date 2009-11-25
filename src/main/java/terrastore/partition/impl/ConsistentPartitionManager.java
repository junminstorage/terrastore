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
package terrastore.partition.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.communication.Node;
import terrastore.partition.PartitionManager;

/**
 * {@link terrastore.partition.PartitionManager} implementation based on consistent ordering (similar to consistent hashing),
 * with equally distributed, ordered, partitioning of nodes.<br>
 * All nodes have at most ((maxPartitions / totalNodes) + (maxPartitions % totalNodes)) assigned partitions.<br>
 * Every node add/remove causes (maxPartitions / totalNodes) partitions reassignment.<br>
 * Thanks to nodes ordering, all nodes will have a consistent, equal, partition table regardless of the actual time
 * nodes joined or left the cluster.
 *
 * @author Sergio Bossa
 */
public class ConsistentPartitionManager implements PartitionManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConsistentPartitionManager.class);
    private final int maxPartitions;
    private final Node[] ring;
    private final SortedSet<Node> nodes;
    private final Map<Node, List<Integer>> nodesToPartitions;
    private final ReadWriteLock stateLock;

    public ConsistentPartitionManager(int maxPartitions) {
        this.maxPartitions = maxPartitions;
        this.ring = new Node[maxPartitions];
        this.nodes = new TreeSet<Node>(new NodeComparator());
        this.nodesToPartitions = new HashMap();
        this.stateLock = new ReentrantReadWriteLock();
    }

    public int getMaxPartitions() {
        return maxPartitions;
    }

    public void addNode(Node node) {
        stateLock.writeLock().lock();
        try {
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
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    public void removeNode(Node node) {
        stateLock.writeLock().lock();
        try {
            if (nodes.contains(node)) {
                nodes.remove(node);
                rebuildPartitions();
            } else {
                // TODO : use proper exception here?
                throw new IllegalStateException("Not existent node: " + node.getName());
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    public Node selectNodeAtPartition(int partition) {
        stateLock.readLock().lock();
        try {
            if (partition >= 0 && partition < ring.length) {
                Node selected = ring[partition];
                LOG.info("Getting node {} at partition {}", selected, partition);
                return selected;
            } else {
                // TODO : use proper exception here?
                throw new IllegalStateException("Wrong partition number: " + partition);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public List<Integer> getPartitionsForNode(Node node) {
        stateLock.readLock().lock();
        try {
            if (nodes.contains(node)) {
                return Collections.unmodifiableList(nodesToPartitions.get(node));
            } else {
                // TODO : use proper exception here?
                throw new IllegalStateException("Wrong node: " + node.getName());
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public void cleanupPartitions() {
        stateLock.writeLock().lock();
        try {
            nodes.clear();
            nodesToPartitions.clear();
        } finally {
            stateLock.writeLock().unlock();
        }
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

    private static class NodeComparator implements Comparator<Node> {

        public int compare(Node n1, Node n2) {
            return n1.getName().compareTo(n2.getName());
        }
    }
}
