/**
 * Copyright 2009 - 2010 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.partition.impl;

import org.junit.Test;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.router.impl.HashFunction;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class ClusterHashingPartitionerTest {

    @Test
    public void testAddAndRemoveNodeOnOneCluster() {
        Cluster cluster = createMock(Cluster.class);
        expect(cluster.getName()).andReturn("cluster").anyTimes();
        Node node = createMock(Node.class);
        expect(node.getName()).andReturn("node").anyTimes();
        HashFunction fn = createMock(HashFunction.class);
        fn.hash("bucket1", 5);
        expectLastCall().andReturn(0).anyTimes();
        fn.hash("bucket2", 5);
        expectLastCall().andReturn(1).anyTimes();
        fn.hash("bucket3", 5);
        expectLastCall().andReturn(2).anyTimes();
        fn.hash("bucket4", 5);
        expectLastCall().andReturn(3).anyTimes();
        fn.hash("bucket5", 5);
        expectLastCall().andReturn(4).anyTimes();

        replay(cluster, node, fn);

        ClusterHashingPartitioner partitioner = new ClusterHashingPartitioner(5, fn);

        partitioner.addNode(cluster, node);
        for (int i = 0; i < 5; i++) {
            assertSame(node, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        partitioner.removeNode(cluster, node);
        for (int i = 0; i < 5; i++) {
            assertNull(partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }

        verify(cluster, node, fn);
    }

    @Test
    public void testAddAndRemoveNodeOnMoreClusters() {
        Cluster cluster1 = createMock(Cluster.class);
        expect(cluster1.getName()).andReturn("cluster1").anyTimes();
        Cluster cluster2 = createMock(Cluster.class);
        expect(cluster2.getName()).andReturn("cluster2").anyTimes();
        Node node1 = createMock(Node.class);
        expect(node1.getName()).andReturn("node1").anyTimes();
        Node node2 = createMock(Node.class);
        expect(node2.getName()).andReturn("node2").anyTimes();
        HashFunction fn = createMock(HashFunction.class);
        fn.hash("bucket1", 5);
        expectLastCall().andReturn(0).anyTimes();
        fn.hash("bucket2", 5);
        expectLastCall().andReturn(1).anyTimes();
        fn.hash("bucket3", 5);
        expectLastCall().andReturn(2).anyTimes();
        fn.hash("bucket4", 5);
        expectLastCall().andReturn(3).anyTimes();
        fn.hash("bucket5", 5);
        expectLastCall().andReturn(4).anyTimes();

        replay(cluster1, cluster2, node1, node2, fn);

        ClusterHashingPartitioner partitioner = new ClusterHashingPartitioner(5, fn);
        
        partitioner.addNode(cluster1, node1);
        for (int i = 0; i < 5; i++) {
            assertSame(node1, partitioner.getNodeFor(cluster1, "bucket" + (i + 1)));
        }
        partitioner.removeNode(cluster1, node1);
        for (int i = 0; i < 5; i++) {
            assertNull(partitioner.getNodeFor(cluster1, "bucket" + (i + 1)));
        }
        partitioner.addNode(cluster2, node2);
        for (int i = 0; i < 5; i++) {
            assertSame(node2, partitioner.getNodeFor(cluster2, "bucket" + (i + 1)));
        }
        partitioner.removeNode(cluster2, node2);
        for (int i = 0; i < 5; i++) {
            assertNull(partitioner.getNodeFor(cluster2, "bucket" + (i + 1)));
        }

        verify(cluster1, cluster2, node1, node2, fn);
    }

    @Test
    public void testAddUntilReachingPartitionsLimit() {
        Cluster cluster = createMock(Cluster.class);
        expect(cluster.getName()).andReturn("cluster").anyTimes();
        Node node1 = createMock(Node.class);
        expect(node1.getName()).andReturn("node1").anyTimes();
        Node node2 = createMock(Node.class);
        expect(node2.getName()).andReturn("node2").anyTimes();
        Node node3 = createMock(Node.class);
        expect(node3.getName()).andReturn("node3").anyTimes();
        Node node4 = createMock(Node.class);
        expect(node4.getName()).andReturn("node4").anyTimes();
        Node node5 = createMock(Node.class);
        expect(node5.getName()).andReturn("node5").anyTimes();
        HashFunction fn = createMock(HashFunction.class);
        fn.hash("bucket1", 5);
        expectLastCall().andReturn(0).anyTimes();
        fn.hash("bucket2", 5);
        expectLastCall().andReturn(1).anyTimes();
        fn.hash("bucket3", 5);
        expectLastCall().andReturn(2).anyTimes();
        fn.hash("bucket4", 5);
        expectLastCall().andReturn(3).anyTimes();
        fn.hash("bucket5", 5);
        expectLastCall().andReturn(4).anyTimes();

        replay(cluster, node1, node2, node3, node4, node5, fn);

        ClusterHashingPartitioner partitioner = new ClusterHashingPartitioner(5, fn);

        partitioner.addNode(cluster, node1);
        for (int i = 0; i < 5; i++) {
            assertSame(node1, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }

        partitioner.addNode(cluster, node2);
        for (int i = 0; i < 2; i++) {
            assertSame(node1, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 2; i < 5; i++) {
            assertSame(node2, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }

        partitioner.addNode(cluster, node3);
        for (int i = 0; i < 1; i++) {
            assertSame(node1, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 1; i < 2; i++) {
            assertSame(node2, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 2; i < 5; i++) {
            assertSame(node3, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }

        partitioner.addNode(cluster, node4);
        for (int i = 0; i < 1; i++) {
            assertSame(node1, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 1; i < 2; i++) {
            assertSame(node2, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 2; i < 3; i++) {
            assertSame(node3, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 3; i < 5; i++) {
            assertSame(node4, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }

        partitioner.addNode(cluster, node5);
        for (int i = 0; i < 1; i++) {
            assertSame(node1, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 1; i < 2; i++) {
            assertSame(node2, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 2; i < 3; i++) {
            assertSame(node3, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 3; i < 4; i++) {
            assertSame(node4, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 4; i < 5; i++) {
            assertSame(node5, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }

        try {
            partitioner.addNode(cluster, node5);
            fail();
        } catch (Exception ex) {
        }

        verify(cluster, node1, node2, node3, node4, node5, fn);
    }

    @Test
    public void testRemoveMaintainsPartitionOrder() {
        Cluster cluster = createMock(Cluster.class);
        expect(cluster.getName()).andReturn("cluster").anyTimes();
        Node node1 = createMock(Node.class);
        expect(node1.getName()).andReturn("node1").anyTimes();
        Node node2 = createMock(Node.class);
        expect(node2.getName()).andReturn("node2").anyTimes();
        Node node3 = createMock(Node.class);
        expect(node3.getName()).andReturn("node3").anyTimes();
        Node node4 = createMock(Node.class);
        expect(node4.getName()).andReturn("node4").anyTimes();
        Node node5 = createMock(Node.class);
        expect(node5.getName()).andReturn("node5").anyTimes();
        HashFunction fn = createMock(HashFunction.class);
        fn.hash("bucket1", 5);
        expectLastCall().andReturn(0).anyTimes();
        fn.hash("bucket2", 5);
        expectLastCall().andReturn(1).anyTimes();
        fn.hash("bucket3", 5);
        expectLastCall().andReturn(2).anyTimes();
        fn.hash("bucket4", 5);
        expectLastCall().andReturn(3).anyTimes();
        fn.hash("bucket5", 5);
        expectLastCall().andReturn(4).anyTimes();

        replay(cluster, node1, node2, node3, node4, node5, fn);

        ClusterHashingPartitioner partitioner = new ClusterHashingPartitioner(5, fn);

        partitioner.addNode(cluster, node1);
        partitioner.addNode(cluster, node2);
        partitioner.addNode(cluster, node3);
        partitioner.addNode(cluster, node4);
        partitioner.addNode(cluster, node5);

        partitioner.removeNode(cluster, node5);
        for (int i = 0; i < 1; i++) {
            assertSame(node1, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 1; i < 2; i++) {
            assertSame(node2, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 2; i < 3; i++) {
            assertSame(node3, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 3; i < 5; i++) {
            assertSame(node4, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }

        partitioner.removeNode(cluster, node4);
        for (int i = 0; i < 1; i++) {
            assertSame(node1, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 1; i < 2; i++) {
            assertSame(node2, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 2; i < 5; i++) {
            assertSame(node3, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }

        partitioner.removeNode(cluster, node3);
        for (int i = 0; i < 2; i++) {
            assertSame(node1, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }
        for (int i = 2; i < 5; i++) {
            assertSame(node2, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }

        partitioner.removeNode(cluster, node2);
        for (int i = 0; i < 5; i++) {
            assertSame(node1, partitioner.getNodeFor(cluster, "bucket" + (i + 1)));
        }

        verify(cluster, node1, node2, node3, node4, node5, fn);
    }
}
