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
        assertEquals("cluster1", stats.getClusters().get(0).getName());
        assertEquals(1, stats.getClusters().get(0).getNodes().size());
        assertEquals("node1", stats.getClusters().get(0).getNodes().get(0).getName());

        verify(cluster, node, router);
    }
}