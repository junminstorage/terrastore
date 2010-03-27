package terrastore.ensemble;

import java.util.List;
import java.util.Map;

/**
 * @author Sergio Bossa
 */
public class EnsembleConfiguration {

    private String clusterName;
    private String ensembleName;
    private List<String> clusters;
    private JGroupsDiscovery jgroupsDiscovery;
    private StaticDiscovery staticDiscovery;

    public EnsembleConfiguration() {
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getEnsembleName() {
        return ensembleName;
    }

    public void setEnsembleName(String ensembleName) {
        this.ensembleName = ensembleName;
    }

    public List<String> getClusters() {
        return clusters;
    }

    public void setClusters(List<String> clusters) {
        this.clusters = clusters;
    }

    public JGroupsDiscovery getJgroupsDiscovery() {
        return jgroupsDiscovery;
    }

    public void setJgroupsDiscovery(JGroupsDiscovery jgroupsDiscovery) {
        this.jgroupsDiscovery = jgroupsDiscovery;
    }

    public StaticDiscovery getStaticDiscovery() {
        return staticDiscovery;
    }

    public void setStaticDiscovery(StaticDiscovery staticDiscovery) {
        this.staticDiscovery = staticDiscovery;
    }

    public static class JGroupsDiscovery {

        private String host;
        private String port;
        private String initialHosts;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public String getInitialHosts() {
            return initialHosts;
        }

        public void setInitialHosts(String initialHosts) {
            this.initialHosts = initialHosts;
        }
    }

    public static class StaticDiscovery {

        private Map<String, List<String>> hosts;

        public Map<String, List<String>> getHosts() {
            return hosts;
        }

        public void setHosts(Map<String, List<String>> hosts) {
            this.hosts = hosts;
        }
    }
}
