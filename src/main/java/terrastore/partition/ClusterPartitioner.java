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
package terrastore.partition;

import java.util.Set;
import terrastore.communication.Cluster;
import terrastore.communication.Node;

/**
 * The ClusterPartitioner manages cluster nodes by organizing them into a partition table whose size must be
 * greater or equal to the max number of cluster nodes.<br>
 * Each node is assigned to one or more partitions, used by the {@link terrastore.router.Router} to manage
 * routes.
 *
 * @author Sergio Bossa
 */
public interface ClusterPartitioner {

    /**
     * Add a {@link terrastore.communication.Node} to the partition table.
     *
     * @param node The node to add.
     */
    public void addNode(Cluster cluster, Node node);

    /**
     * Remove a {@link terrastore.communication.Node} from the partition table.
     *
     * @param node The node to remove.
     */
    public void removeNode(Cluster cluster, Node node);

    /**
     *
     */
    public Set<Node> getNodesFor(Cluster cluster);

    /**
     *
     */
    public Node getNodeFor(Cluster cluster, String bucket);

    /**
     *
     */
    public Node getNodeFor(Cluster cluster, String bucket, String key);

    /**
     * Cleanup the partition table.
     */
    public void cleanupPartitions();

    /**
     * Get the max number of partitions: number of cluster nodes cannot exceed the max number of partitions.
     *
     * @return The max number of partitions.
     */
    public int getMaxPartitions();
}
