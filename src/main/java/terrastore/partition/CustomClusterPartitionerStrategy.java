package terrastore.partition;

/**
 * @author Sergio Bossa
 */
public interface CustomClusterPartitionerStrategy {

    public Node getNodeFor(String cluster, String bucket);

    public Node getNodeFor(String cluster, String bucket, String key);

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
