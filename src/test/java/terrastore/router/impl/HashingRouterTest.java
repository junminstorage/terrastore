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
package terrastore.router.impl;

import org.junit.Test;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.ensemble.EnsembleManager;
import terrastore.partition.PartitionManager;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class HashingRouterTest {

    private HashingRouter router;

    @Test
    public void testRouteToBucketInLocalCluster() {
        String bucket = "bucket";

        HashFunction fn = createMock(HashFunction.class);
        EnsembleManager ensembleManager = createMock(EnsembleManager.class);
        PartitionManager partitionManager = createMock(PartitionManager.class);
        Cluster cluster = createMock(Cluster.class);
        Node node = createMock(Node.class);

        fn.hash(bucket, 10);
        expectLastCall().andReturn(1).once();
        ensembleManager.getClusterFor(bucket);
        expectLastCall().andReturn(cluster);
        partitionManager.addNode(node);
        expectLastCall().once();
        partitionManager.getMaxPartitions();
        expectLastCall().andReturn(10).once();
        partitionManager.selectNodeAtPartition(1);
        expectLastCall().andReturn(node).once();
        cluster.isLocal();
        expectLastCall().andReturn(true).once();

        replay(fn, ensembleManager, partitionManager, cluster, node);

        router = new HashingRouter(fn, ensembleManager, partitionManager);
        router.addRouteTo(cluster, node);
        assertSame(node, router.routeToNodeFor(bucket));

        verify(fn, ensembleManager, partitionManager, cluster, node);
    }

    @Test
    public void testRouteToBucketAndKeyInLocalCluster() {
        String bucket = "bucket";
        String key = "key";

        HashFunction fn = createMock(HashFunction.class);
        EnsembleManager ensembleManager = createMock(EnsembleManager.class);
        PartitionManager partitionManager = createMock(PartitionManager.class);
        Cluster cluster = createMock(Cluster.class);
        Node node = createMock(Node.class);

        fn.hash(bucket + key, 10);
        expectLastCall().andReturn(1).once();
        ensembleManager.getClusterFor(bucket, key);
        expectLastCall().andReturn(cluster);
        partitionManager.addNode(node);
        expectLastCall().once();
        partitionManager.getMaxPartitions();
        expectLastCall().andReturn(10).once();
        partitionManager.selectNodeAtPartition(1);
        expectLastCall().andReturn(node).once();
        cluster.isLocal();
        expectLastCall().andReturn(true).once();

        replay(fn, ensembleManager, partitionManager, cluster, node);

        router = new HashingRouter(fn, ensembleManager, partitionManager);
        router.addRouteTo(cluster, node);
        assertSame(node, router.routeToNodeFor(bucket, key));

        verify(fn, ensembleManager, partitionManager, cluster, node);
    }
}