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
package terrastore.partition;

/**
 * Implement this interface to provide a custom partitioning strategy for distributing
 * bucket and documents among ensemble clusters.
 *
 * @author Sergio Bossa
 */
public interface CustomEnsemblePartitionerStrategy {

    /**
     * Get the {@link Cluster} object where to place the given bucket.
     *
     * @param bucket The bucket to place.
     * @return The {@link Cluster} object where to place the given bucket.
     */
    public Cluster getClusterFor(String bucket);

    /**
     * Get the {@link Cluster} object where to place the given key (under the given bucket).
     *
     * @param bucket The bucket holding the key to place.
     * @param key The key to place
     * @return The {@link Cluster} object where to place the given key.
     */
    public Cluster getClusterFor(String bucket, String key);

    /**
     * Represents a specific cluster where to place buckets and documents.
     */
    public static class Cluster {

        private final String name;

        public Cluster(String name) {
            this.name = name;
        }

        /**
         * Get the unique cluster name.
         * 
         * @return the cluster name.
         */
        public String getName() {
            return name;
        }
    }
}
