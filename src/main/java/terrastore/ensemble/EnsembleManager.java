package terrastore.ensemble;

import java.util.Set;
import terrastore.communication.Cluster;
import terrastore.communication.Node;

/**
 * @author Sergio Bossa
 */
public interface EnsembleManager {

    public void setupClusters(Set<Cluster> clusters);

    public Cluster getClusterFor(String bucket);

    public Cluster getClusterFor(String bucket, String key);

    public void addContactNode(Cluster cluster, Node node);

    public void removeContactNode(Cluster cluster, Node node);

    public Node getContactNodeFor(Cluster cluster);
}
