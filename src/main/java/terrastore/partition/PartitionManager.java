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
package terrastore.partition;

import terrastore.communication.Node;
import java.util.List;

/**
 * The PartitionManager manages cluster nodes by organizing them into a partition table whose size must be
 * greater or equal to the max number of cluster nodes.<br>
 * Each node is assigned to one or more partitions, used by the {@link terrastore.router.Router} to manage
 * routes.
 *
 * @author Sergio Bossa
 */
public interface PartitionManager {

    /**
     * Add a {@link terrastore.communication.Node} to the partition table.
     *
     * @param node The node to add.
     */
    public void addNode(Node node);

    /**
     * Remove a {@link terrastore.communication.Node} from the partition table.
     *
     * @param node The node to remove.
     */
    public void removeNode(Node node);

    /**
     * Select the {@link terrastore.communication.Node} belonging to the given partition number.
     *
     * @param partition The partition number whose node must be selected.
     * @return The node at the given partition number, or null if no node is found.
     */
    public Node selectNodeAtPartition(int partition);

    /**
     * Get all partitions for the given {@link terrastore.communication.Node}.
     *
     * @param node The node whose corresponding partitions must be get.
     * @return A list of partition numbers.
     */
    public List<Integer> getPartitionsForNode(Node node);

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
