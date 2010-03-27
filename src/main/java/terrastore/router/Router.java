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
package terrastore.router;

import java.util.Map;
import java.util.Set;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.partition.ClusterPartitioner;
import terrastore.partition.EnsemblePartitioner;

/**
 * Router interface for defining and finding routes to {@link terrastore.communication.Node}s in the cluster.
 *
 * @author Sergio Bossa
 */
public interface Router {

    /**
     *
     */
    public void setupClusters(Set<Cluster> clusters);

    /**
     *
     */
    public Set<Cluster> getClusters();

    /**
     */
    public void addRouteToLocalNode(Node node);

    /**
     * Add a route to the given node.
     *
     * @param node The node to add the route to.
     */
    public void addRouteTo(Cluster cluster, Node node);

    /**
     * Remove a route to the given node.
     *
     * @param node The node whose route must be removed.
     */
    public void removeRouteTo(Cluster cluster, Node node);

    /**
     */
    public Node routeToLocalNode();

    /**
     * Find the route to a specific node for the given bucket name.
     *
     * @param bucket The name of the bucket.
     * @return The corresponding node.
     */
    public Node routeToNodeFor(String bucket) throws MissingRouteException;

    /**
     * Find the route to a specific node for the given bucket name and key.
     *
     * @param bucket The name of the bucket.
     * @param key The key.
     * @return The corresponding node.
     */
    public Node routeToNodeFor(String bucket, String key) throws MissingRouteException;

    /**
     * Find the route to a set of nodes for the given bucket name and set of keys.

     * @param bucket The name of the bucket.
     * @param keys The set of keys.
     * @return A map associating each node to its set of keys.
     */
    public Map<Node, Set<String>> routeToNodesFor(String bucket, Set<String> keys) throws MissingRouteException;

    /**
     * 
     */
    public Set<Node> broadcastRoute() throws MissingRouteException;

    /**
     * Cleanup all routes.
     */
    public void cleanup();

    /**
     * Get the {@link terrastore.partition.ClusterPartitioner} instance managing cluster nodes partitions.
     * 
     * @return The ClusterPartitioner.
     */
    public ClusterPartitioner getClusterPartitioner();

    /**
     * Get the {@link terrastore.partition.ClusterPartitioner} instance managing ensemble clusters partitions.
     *
     * @return The EnsemblePartitioner.
     */
    public EnsemblePartitioner getEnsemblePartitioner();
}
