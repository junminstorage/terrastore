package terrastore.common;

import java.util.HashSet;
import org.junit.Test;
import terrastore.util.collect.Sets;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class ClusterStatsTest {

    @Test
    public void testClusterIsAvailableWithNodes() {
        ClusterStats.Cluster cluster = new ClusterStats.Cluster("cluster-1", Sets.linked(new ClusterStats.Node("node-1", "localhost", 8080)));
        ClusterStats clusterStats = new ClusterStats(Sets.linked(cluster));
        assertEquals(ClusterStats.Status.AVAILABLE, clusterStats.getClusters().iterator().next().getStatus());
    }

    @Test
    public void testClusterIsAvailableWithNoNodes() {
        ClusterStats.Cluster cluster = new ClusterStats.Cluster("cluster-1", new HashSet<ClusterStats.Node>());
        ClusterStats clusterStats = new ClusterStats(Sets.linked(cluster));
        assertEquals(ClusterStats.Status.UNAVAILABLE, clusterStats.getClusters().iterator().next().getStatus());
    }
}