package terrastore.partition;

import java.util.Set;
import terrastore.communication.Cluster;

/**
 * @author Sergio Bossa
 */
public interface EnsemblePartitioner {

    public void setupClusters(Set<Cluster> clusters);

    public Cluster getClusterFor(String bucket);

    public Cluster getClusterFor(String bucket, String key);
}
