/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
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
import terrastore.partition.CustomEnsemblePartitionerStrategy;
import terrastore.store.Key;
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

        CustomEnsemblePartitionerStrategy strategy = createMock(CustomEnsemblePartitionerStrategy.class);
        strategy.getClusterFor("bucket");
        expectLastCall().andReturn(new CustomEnsemblePartitionerStrategy.Cluster("cluster1")).once();
        strategy.getClusterFor("bucket", "key");
        expectLastCall().andReturn(new CustomEnsemblePartitionerStrategy.Cluster("cluster2")).once();

        replay(strategy);

        EnsembleCustomPartitioner partitioner = new EnsembleCustomPartitioner(strategy);
        partitioner.setupClusters(Sets.hash(cluster1, cluster2));

        assertSame(cluster1, partitioner.getClusterFor("bucket"));
        assertSame(cluster2, partitioner.getClusterFor("bucket", new Key("key")));

        verify(strategy);
    }
}