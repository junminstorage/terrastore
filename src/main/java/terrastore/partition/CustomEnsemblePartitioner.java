package terrastore.partition;

/**
 * @author Sergio Bossa
 */
public interface CustomEnsemblePartitioner {

    public Cluster getClusterFor(String bucket);

    public Cluster getClusterFor(String bucket, String key);

    public static class Cluster {

        private final String name;

        public Cluster(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
