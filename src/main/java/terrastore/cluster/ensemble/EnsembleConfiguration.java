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
package terrastore.cluster.ensemble;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Sergio Bossa
 */
public class EnsembleConfiguration {

    private String localCluster;
    private long discoveryInterval;
    private List<String> clusters = new LinkedList<String>();
    private Map<String, String> seeds = new HashMap<String, String>();

    public String getLocalCluster() {
        return localCluster;
    }

    public void setLocalCluster(String localCluster) {
        this.localCluster = localCluster;
    }

    public long getDiscoveryInterval() {
        return discoveryInterval;
    }

    public void setDiscoveryInterval(long discoveryInterval) {
        this.discoveryInterval = discoveryInterval;
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
        validateLocalCluster();
        validateDiscoveryInterval();
        validateClustersContainLocalCluster();
        validatePerClusterSeeds();
    }

    private void validateDiscoveryInterval() {
        if (discoveryInterval <= 0) {
            throw new EnsembleConfigurationException("Discovery interval must be a positive time value (in milliseconds)!");
        }
    }

    private void validateLocalCluster() {
        if (localCluster == null || localCluster.isEmpty()) {
            throw new EnsembleConfigurationException("Local cluster name must not be empty!");
        }
    }

    private void validateClustersContainLocalCluster() {
        if (!clusters.contains(localCluster)) {
            throw new EnsembleConfigurationException("Clusters list must contain specified local cluster name!");
        }
    }

    private void validatePerClusterSeeds() {
        if (seeds.containsKey(localCluster)) {
            throw new EnsembleConfigurationException("Seeds must not contain a seed for the local cluster!");
        }
        for (String cluster : clusters) {
            if (!cluster.equals(localCluster)) {
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
