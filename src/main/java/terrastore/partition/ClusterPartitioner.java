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
package terrastore.partition;

import java.util.Set;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.store.Key;

/**
 * The ClusterPartitioner manages clusters with related set of nodes, by organizing each set of cluster nodes in a
 * separated partition table whose size must be greater or equal to the max number of cluster nodes.
 *
 * @author Sergio Bossa
 */
public interface ClusterPartitioner {

    /**
     * Add a {@link terrastore.communication.Node} to the partition table of the specified {@link terrastore.communication.Cluster}.
     *
     * @param cluster  The cluster whose partition table the node must be added to.
     * @param node The node to add.
     */
    public void addNode(Cluster cluster, Node node);

    /**
     * Remove a {@link terrastore.communication.Node} from the partition table of the specified {@link terrastore.communication.Cluster}.
     *
     * @param cluster  The cluster whose partition table the node must be added from.
     * @param node The node to remove.
     */
    public void removeNode(Cluster cluster, Node node);

    /**
     * Get all {@link terrastore.communication.Node}s belonging to the specified {@link terrastore.communication.Cluster}.
     *
     * @param cluster
     * @return The set of nodes belonging to the given cluster.
     */
    public Set<Node> getNodesFor(Cluster cluster);

    /**
     * Get the node belonging to the specified {@link terrastore.communication.Cluster} and corresponding to the given bucket name.
     *
     * @param cluster
     * @param bucket
     * @return The cluster node corresponding to the given bucket.
     */
    public Node getNodeFor(Cluster cluster, String bucket);

    /**
     * Get the node belonging to the specified {@link terrastore.communication.Cluster} and corresponding to the given bucket name and key.
     *
     * @param cluster
     * @param bucket
     * @return The cluster node corresponding to the given bucket and key.
     */
    public Node getNodeFor(Cluster cluster, String bucket, Key key);

    /**
     * Cleanup all partition tables.
     */
    public void cleanupPartitions();

    /**
     * Get the max number of partitions: number of nodes (per cluster) cannot exceed the max number of partitions.
     *
     * @return The max number of partitions (per cluster).
     */
    public int getMaxPartitions();
}
