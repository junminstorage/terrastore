package terrastore.ensemble;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import terrastore.ensemble.support.EnsembleConfigurationException;

/**
 * @author Sergio Bossa
 */
public class EnsembleConfiguration {

    private String clusterName;
    private String ensembleName;
    private List<String> clusters = new LinkedList<String>();
    private Map<String, String> seeds = new HashMap<String, String>();

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

    public Map<String, String> getSeeds() {
        return seeds;
    }

    public void setSeeds(Map<String, String> seeds) {
        this.seeds = seeds;
    }

    public void validate() {
        validateClusterAndEnsembleName();
        validateClustersContainLocalCluster();
        validatePerClusterSeeds();
    }

    private void validateClusterAndEnsembleName() {
        if (clusterName == null || clusterName.isEmpty()) {
            throw new EnsembleConfigurationException("Cluster name must not be empty!");
        }
        if (ensembleName == null || ensembleName.isEmpty()) {
            throw new EnsembleConfigurationException("Ensemble name must not be empty!");
        }
    }

    private void validateClustersContainLocalCluster() {
        if (!clusters.contains(clusterName)) {
            throw new EnsembleConfigurationException("Clusters list must contain specified cluster name!");
        }
    }

    private void validatePerClusterSeeds() {
        if (seeds.containsKey(clusterName)) {
            throw new EnsembleConfigurationException("Seeds must not contain a seed for the local cluster!");
        }
        for (String cluster : clusters) {
            if (!cluster.equals(clusterName)) {
                if (!seeds.containsKey(cluster)) {
                    throw new EnsembleConfigurationException("Unable to find a seed for cluster: " + cluster);
                }
                if (!seeds.get(cluster).matches(".+:\\d+")) {
                    throw new EnsembleConfigurationException("Bad seed format (host:port): " + seeds.get(cluster));
                }
            }
        }
    }
}
