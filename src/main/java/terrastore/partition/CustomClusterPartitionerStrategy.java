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
package terrastore.partition;

/**
 * Implement this interface to provide a custom partitioning strategy for distributing
 * bucket and documents among custer nodes.
 *
 * @author Sergio Bossa
 */
public interface CustomClusterPartitionerStrategy {

    /**
     * Get the {@link Node} object where to place the given bucket.<br>
     * Be aware: the returned node must belong to the given cluster.
     *
     * @param cluster The name of the cluster holding the node.
     * @param bucket The bucket to place.
     * @return The {@link Node} object where to place the given bucket.
     */
    public Node getNodeFor(String cluster, String bucket);

    /**
     * Get the {@link Node} object where to place the given key (under the given bucket).<br>
     * Be aware: the returned node must belong to the given cluster.
     *
     * @param cluster The name of the cluster holding the node.
     * @param bucket The bucket holding the key to place.
     * @param key The key to place
     * @return The {@link Node} object where to place the given key.
     */
    public Node getNodeFor(String cluster, String bucket, String key);

    /**
     * Represents a specific node where to place buckets and documents.
     */
    public static class Node {

        private final String host;
        private final int port;

        public Node(String host, int port) {
            this.host = host;
            this.port = port;
        }

        /**
         * Get the node host.
         *
         * @return The node host.
         */
        public String getHost() {
            return host;
        }

        /**
         * Get the node port, that is, the port used for node communication (rather than the HTTP port).
         *
         * @return The node port.
         */
        public int getPort() {
            return port;
        }
    }
}
