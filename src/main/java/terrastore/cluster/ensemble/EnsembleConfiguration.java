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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Sergio Bossa
 */
public class EnsembleConfiguration {

    private DiscoveryConfiguration discovery;
    private String localCluster;
    private List<String> clusters = new LinkedList<String>();
    private Map<String, String> seeds = new HashMap<String, String>();

    public static EnsembleConfiguration makeDefault(String clusterName) {
        DiscoveryConfiguration discoveryConfiguration = new DiscoveryConfiguration();
        discoveryConfiguration.setType("fixed");
        discoveryConfiguration.setInterval(10000L);
        EnsembleConfiguration ensembleConfiguration = new EnsembleConfiguration();
        ensembleConfiguration.setLocalCluster(clusterName);
        ensembleConfiguration.setClusters(Arrays.asList(clusterName));
        ensembleConfiguration.setDiscovery(discoveryConfiguration);
        return ensembleConfiguration;
    }

    public DiscoveryConfiguration getDiscovery() {
        return discovery;
    }

    public void setDiscovery(DiscoveryConfiguration discovery) {
        this.discovery = discovery;
    }

    public String getLocalCluster() {
        return localCluster;
    }

    public void setLocalCluster(String localCluster) {
        this.localCluster = localCluster;
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
        validateDiscoveryConfiguration();
        validateLocalCluster();
        validateClustersContainLocalCluster();
        validatePerClusterSeeds();
    }

    private void validateDiscoveryConfiguration() {
        if (discovery == null) {
            throw new EnsembleConfigurationException("No discovery configuration provided!");
        } else {
            discovery.validate();
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

    public static class DiscoveryConfiguration {

        private String type = "fixed";
        private Long interval;
        private Long baseline;
        private Long increment;
        private Long limit;

        public void validate() {
            validateForFixedScheduler();
            validateForAdaptiveScheduler();
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Long getInterval() {
            return interval;
        }

        public void setInterval(Long interval) {
            this.interval = interval;
        }

        public Long getBaseline() {
            return baseline;
        }

        public void setBaseline(Long baseline) {
            this.baseline = baseline;
        }

        public Long getIncrement() {
            return increment;
        }

        public void setIncrement(Long increment) {
            this.increment = increment;
        }

        public Long getLimit() {
            return limit;
        }

        public void setLimit(Long limit) {
            this.limit = limit;
        }

        private void validateForFixedScheduler() {
            if (type.equals("fixed") && (interval == null || interval <= 0)) {
                throw new EnsembleConfigurationException("Interval must be a positive time value (in milliseconds)!");
            }
        }

        private void validateForAdaptiveScheduler() {
            if (type.equals("adaptive")) {
                if (interval == null || interval <= 0) {
                    throw new EnsembleConfigurationException("Interval must be a positive time value (in milliseconds)!");
                }
                if (baseline == null || baseline <= 0) {
                    throw new EnsembleConfigurationException("Baseline must be a positive time value (in milliseconds)!");
                }
                if (increment == null || increment <= 0) {
                    throw new EnsembleConfigurationException("Upbouns increment must be a positive time value (in milliseconds)!");
                }
                if (limit == null || limit <= 0) {
                    throw new EnsembleConfigurationException("Upbound limit must be a positive time value (in milliseconds)!");
                }
            }
        }

    }
}
