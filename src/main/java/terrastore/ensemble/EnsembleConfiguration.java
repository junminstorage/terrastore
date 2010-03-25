package terrastore.ensemble;

import java.util.List;

/**
 * @author Sergio Bossa
 */
public class EnsembleConfiguration {

    private String clusterName;
    private String ensembleName;
    private String discoveryHost;
    private String discoveryPort;
    private String initialHosts;
    private List<String> clusters;

    public EnsembleConfiguration() {
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getDiscoveryHost() {
        return discoveryHost;
    }

    public void setDiscoveryHost(String discoveryHost) {
        this.discoveryHost = discoveryHost;
    }

    public String getDiscoveryPort() {
        return discoveryPort;
    }

    public void setDiscoveryPort(String discoveryPort) {
        this.discoveryPort = discoveryPort;
    }

    public String getEnsembleName() {
        return ensembleName;
    }

    public void setEnsembleName(String ensembleName) {
        this.ensembleName = ensembleName;
    }

    public String getInitialHosts() {
        return initialHosts;
    }

    public void setInitialHosts(String initialHosts) {
        this.initialHosts = initialHosts;
    }

    public List<String> getClusters() {
        return clusters;
    }

    public void setClusters(List<String> clusters) {
        this.clusters = clusters;
    }
}
