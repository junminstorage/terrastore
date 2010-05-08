package terrastore.partition.impl;

import org.junit.Test;
import terrastore.communication.Cluster;
import terrastore.partition.CustomEnsemblePartitioner;
import terrastore.util.collect.Sets;
import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class EnsembleCustomPartitionerTest {

    @Test
    public void testPartitioning() {
        Cluster cluster1 = new Cluster("cluster1", true);
        Cluster cluster2 = new Cluster("cluster2", false);

        CustomEnsemblePartitioner strategy = createMock(CustomEnsemblePartitioner.class);
        strategy.getClusterFor("bucket");
        expectLastCall().andReturn(new CustomEnsemblePartitioner.Cluster("cluster1")).once();
        strategy.getClusterFor("bucket", "key");
        expectLastCall().andReturn(new CustomEnsemblePartitioner.Cluster("cluster2")).once();

        replay(strategy);

        EnsembleCustomPartitioner partitioner = new EnsembleCustomPartitioner(strategy);
        partitioner.setupClusters(Sets.hash(cluster1, cluster2));

        assertSame(cluster1, partitioner.getClusterFor("bucket"));
        assertSame(cluster2, partitioner.getClusterFor("bucket", "key"));

        verify(strategy);
    }
}