package terrastore.monitoring.jmx;

import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectName;
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

    private final static String TERRASTORE_CLUSTER_DOMAIN = "terrastore.monitoring.cluster";
    private final static String TERRASTORE_CLUSTER_TYPE = "Cluster";
    private final static String TERRASTORE_NODE_TYPE = "Cluster.Node";
    private final MBeanServer mbeanServer;
    private final Router router;

    public JMXClusterMonitoring(MBeanServer mbeanServer, Router router) {
        this.mbeanServer = mbeanServer;
        this.router = router;
    }

    public void update() throws Exception {
        cleanup();
        registerClusters();
    }

    private void cleanup() throws Exception {
        for (ObjectName name : mbeanServer.queryNames(new ObjectName(TERRASTORE_CLUSTER_DOMAIN + "*" + ":*"), null)) {
            mbeanServer.unregisterMBean(name);
        }
    }

    private void registerClusters() throws Exception {
        for (Cluster cluster : router.getClusters()) {
            StringBuilder nameBuilder = new StringBuilder(TERRASTORE_CLUSTER_DOMAIN)
                    .append(":type=").append(TERRASTORE_CLUSTER_TYPE).append(",")
                    .append("name=").append(cluster.getName());
            ObjectName name = new ObjectName(nameBuilder.toString());
            Set<Node> nodes = router.clusterRoute(cluster);
            String status = nodes.isEmpty() ? MClusterMXBean.Status.UNAVAILABLE.toString() : MClusterMXBean.Status.AVAILABLE.toString();
            mbeanServer.registerMBean(new MCluster(cluster.getName(), status), name);
            registerClusterNodes(cluster, nodes);
        }
    }

    private void registerClusterNodes(Cluster cluster, Set<Node> nodes) throws Exception {
        for (Node node : nodes) {
            StringBuilder nameBuilder = new StringBuilder(TERRASTORE_CLUSTER_DOMAIN).append(".").append(cluster.getName())
                    .append(":type=").append(TERRASTORE_NODE_TYPE).append(",")
                    .append("name=").append(node.getName());
            ObjectName name = new ObjectName(nameBuilder.toString());
            mbeanServer.registerMBean(new MNode(node.getName(), node.getHost()), name);
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
        public String toString() {
            return new ToStringBuilder(this).append(name).append(host).toString();
        }
    }
}
