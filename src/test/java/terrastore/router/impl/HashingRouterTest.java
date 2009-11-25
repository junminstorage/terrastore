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
package terrastore.router.impl;

import org.junit.Test;
import terrastore.communication.Node;
import terrastore.partition.PartitionManager;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class HashingRouterTest {

    private HashingRouter router;

    @Test
    public void testRouteToBucket() {
        String bucket = "bucket";

        HashFunction fn = createMock(HashFunction.class);
        PartitionManager partitionManager = createMock(PartitionManager.class);
        Node node = createMock(Node.class);

        partitionManager.addNode(node);
        expectLastCall().once();
        fn.hash(bucket);
        expectLastCall().andReturn(11).once();
        partitionManager.getMaxPartitions();
        expectLastCall().andReturn(10).once();
        partitionManager.selectNodeAtPartition(11 % 10);
        expectLastCall().andReturn(node).once();

        replay(fn, partitionManager, node);

        router = new HashingRouter(fn, partitionManager);
        router.addRouteTo(node);
        assertSame(node, router.routeToNodeFor(bucket));

        verify(fn, partitionManager, node);
    }

    @Test
    public void testRouteToBucketAndKey() {
        String bucket = "bucket";
        String key = "key";

        HashFunction fn = createMock(HashFunction.class);
        PartitionManager partitionManager = createMock(PartitionManager.class);
        Node node = createMock(Node.class);

        partitionManager.addNode(node);
        expectLastCall().once();
        fn.hash(bucket + key);
        expectLastCall().andReturn(11).once();
        partitionManager.getMaxPartitions();
        expectLastCall().andReturn(10).once();
        partitionManager.selectNodeAtPartition(11 % 10);
        expectLastCall().andReturn(node).once();

        replay(fn, partitionManager, node);

        router = new HashingRouter(fn, partitionManager);
        router.addRouteTo(node);
        assertSame(node, router.routeToNodeFor(bucket, key));

        verify(fn, partitionManager, node);
    }
}