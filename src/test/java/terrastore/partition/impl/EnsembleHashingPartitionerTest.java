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
import terrastore.router.impl.HashFunction;
import terrastore.store.Key;
import terrastore.util.collect.Sets;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class EnsembleHashingPartitionerTest {

    @Test
    public void testPartitioner() {
        Cluster cluster1 = createMock(Cluster.class);
        expect(cluster1.getName()).andReturn("cluster1").anyTimes();
        Cluster cluster2 = createMock(Cluster.class);
        expect(cluster2.getName()).andReturn("cluster2").anyTimes();
        Cluster cluster3 = createMock(Cluster.class);
        expect(cluster3.getName()).andReturn("cluster3").anyTimes();
        HashFunction fn = createMock(HashFunction.class);
        fn.hash("bucket1", 3);
        expectLastCall().andReturn(0).once();
        fn.hash("bucket2", 3);
        expectLastCall().andReturn(1).once();
        fn.hash("bucket3", 3);
        expectLastCall().andReturn(2).once();

        replay(cluster1, cluster2, cluster3, fn);

        EnsembleHashingPartitioner partitioner = new EnsembleHashingPartitioner(fn);

        partitioner.setupClusters(Sets.hash(cluster1, cluster2, cluster3));
        
        assertSame(cluster1, partitioner.getClusterFor("bucket", new Key("1")));
        assertSame(cluster2, partitioner.getClusterFor("bucket", new Key("2")));
        assertSame(cluster3, partitioner.getClusterFor("bucket", new Key("3")));

        verify(cluster1, cluster2, cluster3, fn);
    }
}
