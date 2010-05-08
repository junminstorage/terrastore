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
