/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
import terrastore.communication.Node;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class ConsistentPartitionManagerTest {

    private ConsistentPartitionManager partitionManager;

    @Test
    public void testAddAndRemoveSingleNode() {
        Node node = createMock(Node.class);
        expect(node.getName()).andReturn("node").anyTimes();

        replay(node);

        partitionManager = new ConsistentPartitionManager(10);
        partitionManager.addNode(node);
        for (int i = 0; i < 10; i++) {
            assertSame(node, partitionManager.selectNodeAtPartition(i));
        }
        partitionManager.removeNode(node);
        for (int i = 0; i < 10; i++) {
            assertNull(partitionManager.selectNodeAtPartition(i));
        }

        verify(node);
    }

    @Test
    public void testAddUntilReachingPartitionsLimit() {
        Node node1 = createMock(Node.class);
        Node node2 = createMock(Node.class);
        Node node3 = createMock(Node.class);
        Node node4 = createMock(Node.class);
        Node node5 = createMock(Node.class);
        expect(node1.getName()).andReturn("node1").anyTimes();
        expect(node2.getName()).andReturn("node2").anyTimes();
        expect(node3.getName()).andReturn("node3").anyTimes();
        expect(node4.getName()).andReturn("node4").anyTimes();
        expect(node5.getName()).andReturn("node5").anyTimes();

        replay(node1, node2, node3, node4, node5);

        partitionManager = new ConsistentPartitionManager(5);

        partitionManager.addNode(node1);
        for (int i = 0; i < 5; i++) {
            assertSame(node1, partitionManager.selectNodeAtPartition(i));
        }

        partitionManager.addNode(node2);
        for (int i = 0; i < 2; i++) {
            assertSame(node1, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 2; i < 5; i++) {
            assertSame(node2, partitionManager.selectNodeAtPartition(i));
        }

        partitionManager.addNode(node3);
        for (int i = 0; i < 1; i++) {
            assertSame(node1, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 1; i < 2; i++) {
            assertSame(node2, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 2; i < 5; i++) {
            assertSame(node3, partitionManager.selectNodeAtPartition(i));
        }

        partitionManager.addNode(node4);
        for (int i = 0; i < 1; i++) {
            assertSame(node1, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 1; i < 2; i++) {
            assertSame(node2, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 2; i < 3; i++) {
            assertSame(node3, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 3; i < 5; i++) {
            assertSame(node4, partitionManager.selectNodeAtPartition(i));
        }

        partitionManager.addNode(node5);
        for (int i = 0; i < 1; i++) {
            assertSame(node1, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 1; i < 2; i++) {
            assertSame(node2, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 2; i < 3; i++) {
            assertSame(node3, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 3; i < 4; i++) {
            assertSame(node4, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 4; i < 5; i++) {
            assertSame(node5, partitionManager.selectNodeAtPartition(i));
        }

        try {
            partitionManager.addNode(node5);
            fail();
        } catch (Exception ex) {
        }

        verify(node1, node2, node3, node4, node5);
    }

    @Test
    public void testRemoveMaintainsPartitionOrder() {
        Node node1 = createMock(Node.class);
        Node node2 = createMock(Node.class);
        Node node3 = createMock(Node.class);
        Node node4 = createMock(Node.class);
        Node node5 = createMock(Node.class);
        expect(node1.getName()).andReturn("node1").anyTimes();
        expect(node2.getName()).andReturn("node2").anyTimes();
        expect(node3.getName()).andReturn("node3").anyTimes();
        expect(node4.getName()).andReturn("node4").anyTimes();
        expect(node5.getName()).andReturn("node5").anyTimes();

        replay(node1, node2, node3, node4, node5);

        partitionManager = new ConsistentPartitionManager(5);
        partitionManager.addNode(node1);
        partitionManager.addNode(node2);
        partitionManager.addNode(node3);
        partitionManager.addNode(node4);
        partitionManager.addNode(node5);

        partitionManager.removeNode(node5);
        for (int i = 0; i < 1; i++) {
            assertSame(node1, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 1; i < 2; i++) {
            assertSame(node2, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 2; i < 3; i++) {
            assertSame(node3, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 3; i < 5; i++) {
            assertSame(node4, partitionManager.selectNodeAtPartition(i));
        }

        partitionManager.removeNode(node4);
        for (int i = 0; i < 1; i++) {
            assertSame(node1, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 1; i < 2; i++) {
            assertSame(node2, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 2; i < 5; i++) {
            assertSame(node3, partitionManager.selectNodeAtPartition(i));
        }

        partitionManager.removeNode(node3);
        for (int i = 0; i < 2; i++) {
            assertSame(node1, partitionManager.selectNodeAtPartition(i));
        }
        for (int i = 2; i < 5; i++) {
            assertSame(node2, partitionManager.selectNodeAtPartition(i));
        }

        partitionManager.removeNode(node2);
        for (int i = 0; i < 5; i++) {
            assertSame(node1, partitionManager.selectNodeAtPartition(i));
        }

        verify(node1, node2, node3, node4, node5);
    }
}