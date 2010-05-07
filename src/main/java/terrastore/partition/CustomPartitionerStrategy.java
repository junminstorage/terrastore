package terrastore.partition;

/**
 * @author Sergio Bossa
 */
public interface CustomPartitionerStrategy {

    public Cluster getClusterFor(String bucket);

    public Cluster getClusterFor(String bucket, String key);

    public Node getNodeFor(String cluster, String bucket);

    public Node getNodeFor(String cluster, String bucket, String key);

    public static class Cluster {

        private final String name;

        public Cluster(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static class Node {

        private final String host;
        private final int port;

        public Node(String host, int port) {
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
