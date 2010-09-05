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
package terrastore.service.impl;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import terrastore.common.ClusterStats;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.partition.ClusterPartitioner;
import terrastore.router.Router;
import terrastore.service.StatsService;

/**
 * 
 * @author Giuseppe Santoro
 * 
 */
public class DefaultStatsService implements StatsService {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStatsService.class);
    private final Router router;

    public DefaultStatsService(Router router) {
        this.router = router;
    }

    @Override
    public ClusterStats getClusterStats() {
        LOG.debug("Getting cluster statistics.");
        ClusterStats stats = new ClusterStats();
        Set<Cluster> clusters = router.getClusters();
        ClusterPartitioner clusterPartitioner = router.getClusterPartitioner();
        for (Cluster cluster : clusters) {
            ClusterStats.Cluster c = stats.new Cluster(cluster.getName());
            Set<Node> nodesForCluster = clusterPartitioner.getNodesFor(cluster);
            for (Node node : nodesForCluster) {
                c.getNodes().add(stats.new Node(node.getName(), node.getHost(), node.getPort()));
            }
            stats.getClusters().add(c);
        }
        return stats;
    }

}
