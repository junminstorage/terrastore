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
package terrastore.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

/**
 * @author Giuseppe Santoro
 */
public class ClusterStats implements Serializable {

    private List<ClusterStats.Cluster> clusters = new ArrayList<ClusterStats.Cluster>();

    public void setClusters(List<ClusterStats.Cluster> clusters) {
        this.clusters = clusters;
    }

    public List<ClusterStats.Cluster> getClusters() {
        return clusters;
    }

    @JsonPropertyOrder({"name", "nodes"})
    public static class Cluster {

        private String name;
        private List<Node> nodes = new ArrayList<Node>();

        public Cluster() {
        }

        public Cluster(String name) {
            this.name = name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setNodes(List<Node> nodes) {
            this.nodes = nodes;
        }

        public List<Node> getNodes() {
            return nodes;
        }
    }

    @JsonPropertyOrder({"name", "host", "port"})
    public static class Node {

        private String name;
        private String host;
        private int port;

        public Node() {
        }

        public Node(String name, String host, int port) {
            this.name = name;
            this.host = host;
            this.port = port;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getHost() {
            return host;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getPort() {
            return port;
        }
    }
}
