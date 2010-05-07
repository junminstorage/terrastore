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
 * Router interface for defining and finding routes to ensemble {@link terrastore.communication.Cluster}s
 * and related {@link terrastore.communication.Node}s.
 *
 * @author Sergio Bossa
 */
public interface Router {

    /**
     * Set up all {@link terrastore.communication.Cluster}s which make up the ensemble.
     *
     * @param clusters
     */
    public void setupClusters(Set<Cluster> clusters);

    /**
     * Get all {@link terrastore.communication.Cluster}s belonging to the ensemble.
     *
     * @return All clusters.
     */
    public Set<Cluster> getClusters();

    /**
     * Add a route to the {@link terrastore.communication.Node} object representing this local node.
     *
     * @param node The local node object.
     */
    public void addRouteToLocalNode(Node node);

    /**
     * Add a route to the given {@link terrastore.communication.Node} belonging to the given {@link terrastore.communication.Cluster}.
     *
     * @param cluster The cluster of the node to add.
     * @param node The node to add the route to.
     */
    public void addRouteTo(Cluster cluster, Node node);

    /**
     * Remove a route to the given {@link terrastore.communication.Node} belonging to the given {@link terrastore.communication.Cluster}.
     *
     * @param cluster The cluster of the node to remove.
     * @param node The node whose route must be removed.
     */
    public void removeRouteTo(Cluster cluster, Node node);

    /**
     * Find the route to this local {@link terrastore.communication.Node}.
     *
     * @return The route to the local node.
     */
    public Node routeToLocalNode();

    /**
     * Find the route to a specific node for the given bucket name.
     *
     * @param bucket The name of the bucket.
     * @return The corresponding node.
     * @throws MissingRouteException If no route can be found.
     */
    public Node routeToNodeFor(String bucket) throws MissingRouteException;

    /**
     * Find the route to a specific node for the given bucket name and key.
     *
     * @param bucket The name of the bucket.
     * @param key The key.
     * @return The corresponding node.
     * @throws MissingRouteException If no route can be found.
     */
    public Node routeToNodeFor(String bucket, String key) throws MissingRouteException;

    /**
     * Find the route to a set of nodes for the given bucket name and set of keys.

     * @param bucket The name of the bucket.
     * @param keys The set of keys.
     * @return A map associating each node to its set of keys.
     * @throws MissingRouteException If no route can be found.
     */
    public Map<Node, Set<String>> routeToNodesFor(String bucket, Set<String> keys) throws MissingRouteException;

    /**
     * Find the route for all {@link terrastore.communication.Node}s belonging to the given {@link terrastore.communication.Cluster}.
     *
     * @param cluster The cluster whose nodes must be returned.
     * @return A set of all nodes belonging to the cluster:
     * the actual order of returned nodes depends on the actual Router implementation.
     */
    public Set<Node> clusterRoute(Cluster cluster);

    /**
     * Find the route for all {@link terrastore.communication.Node}s of all {@link terrastore.communication.Cluster}s.
     * @return A map containing all clusters with the related set of nodes:
     * the actual order of returned nodes per cluster depends on the actual Router implementation
     */
    public Map<Cluster, Set<Node>> broadcastRoute();

    /**
     * Cleanup all routes.
     */
    public void cleanup();

    /**
     * Get the {@link terrastore.partition.ClusterPartitioner} instance managing per-cluster nodes partitions.
     * 
     * @return
     */
    public ClusterPartitioner getClusterPartitioner();

    /**
     * Get the {@link terrastore.partition.ClusterPartitioner} instance managing ensemble clusters partitions.
     *
     * @return
     */
    public EnsemblePartitioner getEnsemblePartitioner();
}
