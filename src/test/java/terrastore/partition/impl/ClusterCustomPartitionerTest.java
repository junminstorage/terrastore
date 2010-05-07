package terrastore.partition.impl;

import org.junit.Test;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class ClusterCustomPartitionerTest {

    @Test
    public void testAddAndRemoveNodesOnSameCluster() {
        Cluster cluster = createMock(Cluster.class);
        expect(cluster.getName()).andReturn("cluster").anyTimes();
        Node node1 = createMock(Node.class);
        expect(node1.getHost()).andReturn("host1").anyTimes();
        expect(node1.getPort()).andReturn(8000).anyTimes();
        Node node2 = createMock(Node.class);
        expect(node2.getHost()).andReturn("host2").anyTimes();
        expect(node2.getPort()).andReturn(8000).anyTimes();

        ClusterPartitionerStrategy strategy = createMock(ClusterPartitionerStrategy.class);

        replay(cluster, node1, node2, strategy);

        ClusterCustomPartitioner partitioner = new ClusterCustomPartitioner(strategy);

        partitioner.addNode(cluster, node1);
        partitioner.addNode(cluster, node2);
        assertEquals(2, partitioner.getNodesFor(cluster).size());
        assertTrue(partitioner.getNodesFor(cluster).contains(node1));
        assertTrue(partitioner.getNodesFor(cluster).contains(node2));

        partitioner.removeNode(cluster, node1);
        partitioner.removeNode(cluster, node2);
        assertEquals(0, partitioner.getNodesFor(cluster).size());

        verify(cluster, node1, node2, strategy);
    }

    @Test
    public void testAddAndRemoveNodesOnDifferentClusters() {
        Cluster cluster1 = createMock(Cluster.class);
        expect(cluster1.getName()).andReturn("cluster1").anyTimes();
        Cluster cluster2 = createMock(Cluster.class);
        expect(cluster2.getName()).andReturn("cluster2").anyTimes();
        Node node1 = createMock(Node.class);
        expect(node1.getHost()).andReturn("host1").anyTimes();
        expect(node1.getPort()).andReturn(8000).anyTimes();
        Node node2 = createMock(Node.class);
        expect(node2.getHost()).andReturn("host2").anyTimes();
        expect(node2.getPort()).andReturn(8000).anyTimes();

        ClusterPartitionerStrategy strategy = createMock(ClusterPartitionerStrategy.class);

        replay(cluster1, cluster2, node1, node2, strategy);

        ClusterCustomPartitioner partitioner = new ClusterCustomPartitioner(strategy);

        partitioner.addNode(cluster1, node1);
        partitioner.addNode(cluster2, node2);
        assertEquals(1, partitioner.getNodesFor(cluster1).size());
        assertEquals(1, partitioner.getNodesFor(cluster2).size());
        assertTrue(partitioner.getNodesFor(cluster1).contains(node1));
        assertTrue(partitioner.getNodesFor(cluster2).contains(node2));

        partitioner.removeNode(cluster1, node1);
        partitioner.removeNode(cluster2, node2);
        assertEquals(0, partitioner.getNodesFor(cluster1).size());
        assertEquals(0, partitioner.getNodesFor(cluster2).size());

        verify(cluster1, cluster2, node1, node2, strategy);
    }

    @Test
    public void testPartitioning() {
        Cluster cluster1 = createMock(Cluster.class);
        expect(cluster1.getName()).andReturn("cluster1").anyTimes();
        Cluster cluster2 = createMock(Cluster.class);
        expect(cluster2.getName()).andReturn("cluster2").anyTimes();
        Node node1 = createMock(Node.class);
        expect(node1.getHost()).andReturn("host1").anyTimes();
        expect(node1.getPort()).andReturn(8000).anyTimes();
        Node node2 = createMock(Node.class);
        expect(node2.getHost()).andReturn("host2").anyTimes();
        expect(node2.getPort()).andReturn(8000).anyTimes();

        ClusterPartitionerStrategy strategy = createMock(ClusterPartitionerStrategy.class);
        strategy.getPartitionFor("cluster1", "bucket1");
        expectLastCall().andReturn(new ClusterPartitionerStrategy.Partition("host1", 8000)).once();
        strategy.getPartitionFor("cluster2", "bucket2");
        expectLastCall().andReturn(new ClusterPartitionerStrategy.Partition("host2", 8000)).once();
        strategy.getPartitionFor("cluster1", "bucket1", "key1");
        expectLastCall().andReturn(new ClusterPartitionerStrategy.Partition("host1", 8000)).once();
        strategy.getPartitionFor("cluster2", "bucket2", "key2");
        expectLastCall().andReturn(new ClusterPartitionerStrategy.Partition("host2", 8000)).once();

        replay(cluster1, cluster2, node1, node2, strategy);

        ClusterCustomPartitioner partitioner = new ClusterCustomPartitioner(strategy);
        partitioner.addNode(cluster1, node1);
        partitioner.addNode(cluster2, node2);

        assertEquals(node1, partitioner.getNodeFor(cluster1, "bucket1"));
        assertEquals(node2, partitioner.getNodeFor(cluster2, "bucket2"));
        assertEquals(node1, partitioner.getNodeFor(cluster1, "bucket1", "key1"));
        assertEquals(node2, partitioner.getNodeFor(cluster2, "bucket2", "key2"));

        verify(cluster1, cluster2, node1, node2, strategy);
    }
}
