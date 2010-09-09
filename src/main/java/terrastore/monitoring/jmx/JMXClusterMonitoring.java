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
package terrastore.monitoring.jmx;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.router.Router;

/**
 * JMX bean to update Terrastore cluster monitoring by adding a specific MBean for every cluster and every node.
 *
 * @author Sergio Bossa
 */
public class JMXClusterMonitoring {

    private final static String CLUSTER_DOMAIN = "terrastore.monitoring.cluster";
    private final static String CLUSTER_TYPE = "Cluster";
    private final static String NODE_TYPE = "Cluster.Node";
    private final Set<MCluster> clusters = new HashSet<MCluster>();
    private final Map<String, Set<MNode>> perClusterNodes = new HashMap<String, Set<MNode>>();
    private final MBeanServer mbeanServer;
    private final Router router;

    public JMXClusterMonitoring(MBeanServer mbeanServer, Router router) {
        this.mbeanServer = mbeanServer;
        this.router = router;
    }

    public synchronized void update() throws Exception {
        registerClusters();
        registerClusterNodes();
    }

    private void registerClusters() throws Exception {
        Set<MCluster> currentMClusters = clusters;
        Set<MCluster> newMClusters = Sets.newHashSet(Iterables.transform(router.getClusters(), new ClusterToMClusterTransformer(router)));
        for (MCluster candidate : currentMClusters) {
            if (!newMClusters.contains(candidate)) {
                StringBuilder objNameBuilder = new StringBuilder(CLUSTER_DOMAIN).append(":type=").append(CLUSTER_TYPE).append(",").append("name=").append(candidate.
                        getName());
                ObjectName objName = new ObjectName(objNameBuilder.toString());
                mbeanServer.unregisterMBean(objName);
            }
        }
        for (MCluster candidate : newMClusters) {
            if (!currentMClusters.contains(candidate)) {
                StringBuilder objNameBuilder = new StringBuilder(CLUSTER_DOMAIN).append(":type=").append(CLUSTER_TYPE).append(",").append("name=").append(candidate.
                        getName());
                ObjectName objName = new ObjectName(objNameBuilder.toString());
                mbeanServer.registerMBean(candidate, objName);
            }
        }
        clusters.clear();
        clusters.addAll(newMClusters);
    }

    private void registerClusterNodes() throws Exception {
        for (Cluster cluster : router.getClusters()) {
            Set<Node> nodes = router.clusterRoute(cluster);
            Set<MNode> currentMNodes = perClusterNodes.get(cluster.getName()) != null ? perClusterNodes.get(cluster.getName()) : new HashSet<MNode>(0);
            Set<MNode> newMNodes = Sets.newHashSet(Iterables.transform(nodes, new NodeToMNodeTransformer()));
            for (MNode candidate : currentMNodes) {
                if (!newMNodes.contains(candidate)) {
                    StringBuilder objNameBuilder = new StringBuilder(CLUSTER_DOMAIN).append(".").append(cluster.getName()).append(":type=").append(NODE_TYPE).
                            append(",").append("name=").append(candidate.getName());
                    ObjectName objName = new ObjectName(objNameBuilder.toString());
                    mbeanServer.unregisterMBean(objName);
                }
            }
            for (MNode candidate : newMNodes) {
                if (!currentMNodes.contains(candidate)) {
                    StringBuilder objNameBuilder = new StringBuilder(CLUSTER_DOMAIN).append(".").append(cluster.getName()).append(":type=").append(NODE_TYPE).
                            append(",").append("name=").append(candidate.getName());
                    ObjectName objName = new ObjectName(objNameBuilder.toString());
                    mbeanServer.registerMBean(candidate, objName);
                }
            }
            perClusterNodes.put(cluster.getName(), newMNodes);
        }
    }

    public static class MCluster implements MClusterMXBean {

        private final String name;
        private final String status;

        public MCluster(String name, String status) {
            this.name = name;
            this.status = status;
        }

        public String getName() {
            return name;
        }

        @Override
        public String getStatus() {
            return status;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj != null && obj instanceof MCluster) {
                MCluster other = (MCluster) obj;
                return new EqualsBuilder().append(this.name, other.name).append(this.status, other.status).isEquals();
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(name).append(status).toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this).append(name).toString();
        }
    }

    public static class MNode implements MNodeMXBean {

        private final String name;
        private final String host;

        public MNode(String name, String host) {
            this.name = name;
            this.host = host;
        }

        public String getName() {
            return name;
        }

        public String getHost() {
            return host;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj != null && obj instanceof MNode) {
                MNode other = (MNode) obj;
                return new EqualsBuilder().append(this.name, other.name).append(this.host, other.host).isEquals();
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(name).append(host).toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this).append(name).append(host).toString();
        }
    }

    private static class ClusterToMClusterTransformer implements Function<Cluster, MCluster> {

        private final Router router;

        public ClusterToMClusterTransformer(Router router) {
            this.router = router;
        }

        @Override
        public MCluster apply(Cluster source) {
            Set<Node> nodes = router.clusterRoute(source);
            String name = source.getName();
            String status = nodes.isEmpty() ? MClusterMXBean.Status.UNAVAILABLE.toString() : MClusterMXBean.Status.AVAILABLE.toString();
            return new MCluster(name, status);
        }
    }

    private static class NodeToMNodeTransformer implements Function<Node, MNode> {

        @Override
        public MNode apply(Node source) {
            return new MNode(source.getName(), source.getHost());
        }
    }
}
