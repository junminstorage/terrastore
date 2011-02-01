/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

/**
 * @author Giuseppe Santoro
 */
public class ClusterStats implements Serializable {

    private Set<ClusterStats.Cluster> clusters = new HashSet<Cluster>();

    public ClusterStats(Set<ClusterStats.Cluster> clusters) {
        this.clusters = clusters;
    }

    public void setClusters(Set<ClusterStats.Cluster> clusters) {
        this.clusters = clusters;
    }

    public Set<ClusterStats.Cluster> getClusters() {
        return clusters;
    }

    @JsonPropertyOrder({"name", "status", "nodes"})
    public static class Cluster {

        private String name;
        private Set<Node> nodes = new HashSet<Node>();
        private Status status;

        protected Cluster() {
        }

        public Cluster(String name, Set<Node> nodes) {
            this.name = name;
            this.nodes = nodes;
            if (nodes.size() > 0) {
                status = Status.AVAILABLE;
            } else {
                status = Status.UNAVAILABLE;
            }
        }

        protected void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        protected void setNodes(Set<Node> nodes) {
            this.nodes = nodes;
        }

        public Set<Node> getNodes() {
            return Collections.unmodifiableSet(nodes);
        }

        protected void setStatus(Status status) {
            this.status = status;
        }

        public Status getStatus() {
            return status;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj != null && obj instanceof Cluster) {
                Cluster other = (Cluster) obj;
                return new EqualsBuilder().append(this.name, other.name).append(this.nodes, other.nodes).append(this.status, other.status).isEquals();
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(name).append(nodes).append(status).toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this).append(name).append(status).toString();
        }
    }

    @JsonPropertyOrder({"name", "host", "port"})
    public static class Node {

        private String name;
        private String host;
        private int port;

        protected Node() {
        }

        public Node(String name, String host, int port) {
            this.name = name;
            this.host = host;
            this.port = port;
        }

        protected void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        protected void setHost(String host) {
            this.host = host;
        }

        public String getHost() {
            return host;
        }

        protected void setPort(int port) {
            this.port = port;
        }

        public int getPort() {
            return port;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj != null && obj instanceof Node) {
                Node other = (Node) obj;
                return new EqualsBuilder().append(this.name, other.name).append(this.host, other.host).append(this.port, other.port).isEquals();
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(name).append(host).append(port).toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this).append(name).append(host).append(port).toString();
        }
    }

    public enum Status {

        AVAILABLE,
        UNAVAILABLE;
    }
}
