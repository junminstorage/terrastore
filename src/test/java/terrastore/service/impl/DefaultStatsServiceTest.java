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
package terrastore.service.impl;

import org.junit.Test;
import terrastore.common.ClusterStats;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.router.Router;
import terrastore.util.collect.Sets;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class DefaultStatsServiceTest {

    @Test
    public void testGetClusterStats() {
        Cluster cluster = createMock(Cluster.class);
        cluster.getName();
        expectLastCall().andReturn("cluster1").once();
        Node node = createMock(Node.class);
        node.getName();
        expectLastCall().andReturn("node1").once();
        node.getHost();
        expectLastCall().andReturn("localhost").once();
        node.getPort();
        expectLastCall().andReturn(8080).once();
        Router router = createMock(Router.class);
        router.getClusters();
        expectLastCall().andReturn(Sets.linked(cluster));
        router.clusterRoute(cluster);
        expectLastCall().andReturn(Sets.linked(node));

        replay(cluster, node, router);

        DefaultStatsService service = new DefaultStatsService(router);
        ClusterStats stats = service.getClusterStats();
        assertEquals(1, stats.getClusters().size());
        assertEquals("cluster1", stats.getClusters().iterator().next().getName());
        assertEquals(1, stats.getClusters().iterator().next().getNodes().size());
        assertEquals("node1", stats.getClusters().iterator().next().getNodes().iterator().next().getName());

        verify(cluster, node, router);
    }
}