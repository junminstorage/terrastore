package terrastore.partition.impl;

/**
 * @author Sergio Bossa
 */
public interface EnsemblePartitionerStrategy {

    public Partition getPartitionFor(String bucket);

    public Partition getPartitionFor(String bucket, String key);

    public static class Partition {

        private final String cluster;

        public Partition(String cluster) {
            this.cluster = cluster;
        }

        public String getCluster() {
            return cluster;
        }
    }
}
