package terrastore.partition;

import java.util.Set;
import terrastore.communication.Cluster;

/**
 * The EnsemblePartitioner manages ensemble clusters, creating a fixed partition table for configured clusters.
 *
 * @author Sergio Bossa
 */
public interface EnsemblePartitioner {

    /**
     * Set up {@link terrastore.communication.Cluster}s in a fixed partition table.
     *
     * @param clusters Clusters to set up.
     */
    public void setupClusters(Set<Cluster> clusters);

    /**
     * Get the {@link terrastore.communication.Cluster} corresponding to the given bucket name.
     *
     * @param bucket
     * @return The cluster corresponding to the given bucket.
     */
    public Cluster getClusterFor(String bucket);

    /**
     * Get the {@link terrastore.communication.Cluster} corresponding to the given bucket name and key.
     *
     * @param bucket
     * @param key
     * @return The cluster corresponding to the given bucket and key.
     */
    public Cluster getClusterFor(String bucket, String key);
}
