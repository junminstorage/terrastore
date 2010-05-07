package terrastore.partition.impl;

/**
 * @author Sergio Bossa
 */
public interface ClusterPartitionerStrategy {

    public Partition getPartitionFor(String cluster, String bucket);

    public Partition getPartitionFor(String cluster, String bucket, String key);

    public static class Partition {

        private final String host;
        private final int port;

        public Partition(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }
}
