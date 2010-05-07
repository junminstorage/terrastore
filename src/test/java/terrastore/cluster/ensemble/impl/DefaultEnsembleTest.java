package terrastore.cluster.ensemble.impl;

import java.util.concurrent.TimeUnit;
import org.easymock.EasyMock;
import org.junit.Test;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.communication.RemoteNodeFactory;
import terrastore.communication.protocol.MembershipCommand;
import terrastore.router.Router;
import terrastore.util.collect.Sets;
import static org.easymock.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class DefaultEnsembleTest {

    @Test
    public void testJoin() throws Exception {
        Cluster cluster = new Cluster("cluster", false);

        Node seed = createMock(Node.class);
        makeThreadSafe(seed, true);
        seed.connect();
        expectLastCall().once();
        seed.send(EasyMock.<MembershipCommand>anyObject());
        expectLastCall().andReturn(new View("cluster", Sets.hash(new View.Member("discovered", "localhost", 6000))));
        seed.disconnect();
        expectLastCall().once();
        //
        Node discoveredNode = createMock(Node.class);
        makeThreadSafe(discoveredNode, true);
        discoveredNode.connect();
        expectLastCall().once();
        discoveredNode.disconnect();
        expectLastCall().once();
        //
        RemoteNodeFactory nodeFactory = createMock(RemoteNodeFactory.class);
        makeThreadSafe(nodeFactory, true);
        nodeFactory.makeRemoteNode("localhost", 6000, "localhost:6000");
        expectLastCall().andReturn(seed).once();
        nodeFactory.makeRemoteNode("localhost", 6000, "discovered");
        expectLastCall().andReturn(discoveredNode).once();
        //
        Router router = createMock(Router.class);
        makeThreadSafe(router, true);
        router.addRouteTo(cluster, discoveredNode);
        expectLastCall().once();

        replay(seed, discoveredNode, nodeFactory, router);

        DefaultEnsemble ensemble = new DefaultEnsemble(router, nodeFactory);
        try {
            ensemble.join(cluster, "localhost:6000");
            Thread.sleep(3000);
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            ensemble.shutdown();
            verify(seed, discoveredNode, nodeFactory, router);
        }
    }

    @Test
    public void testSchedule() throws Exception {
        Cluster cluster = new Cluster("cluster", false);

        Node seed = createMock(Node.class);
        makeThreadSafe(seed, true);
        seed.connect();
        expectLastCall().once();
        seed.send(EasyMock.<MembershipCommand>anyObject());
        expectLastCall().andReturn(new View("cluster", Sets.hash(new View.Member("discovered", "localhost", 6000))));
        seed.disconnect();
        expectLastCall().once();
        //
        Node discoveredNode = createMock(Node.class);
        makeThreadSafe(discoveredNode, true);
        discoveredNode.connect();
        expectLastCall().once();
        discoveredNode.disconnect();
        expectLastCall().once();
        discoveredNode.send(EasyMock.<MembershipCommand>anyObject());
        expectLastCall().andReturn(new View("cluster", Sets.hash(new View.Member("discovered", "localhost", 6000))));
        //
        RemoteNodeFactory nodeFactory = createMock(RemoteNodeFactory.class);
        makeThreadSafe(nodeFactory, true);
        nodeFactory.makeRemoteNode("localhost", 6000, "localhost:6000");
        expectLastCall().andReturn(seed).once();
        nodeFactory.makeRemoteNode("localhost", 6000, "discovered");
        expectLastCall().andReturn(discoveredNode).once();
        //
        Router router = createMock(Router.class);
        makeThreadSafe(router, true);
        router.addRouteTo(cluster, discoveredNode);
        expectLastCall().once();

        replay(seed, discoveredNode, nodeFactory, router);

        DefaultEnsemble ensemble = new DefaultEnsemble(router, nodeFactory);
        try {
            ensemble.join(cluster, "localhost:6000");
            Thread.sleep(3000);
            ensemble.schedule(0, 10, TimeUnit.SECONDS);
            Thread.sleep(3000);
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            ensemble.shutdown();
            verify(seed, discoveredNode, nodeFactory, router);
        }
    }

    @Test
    public void testJoinAndUpdateWithNewConnectedNode() throws Exception {
        Cluster cluster = new Cluster("cluster", false);

        Node seed = createMock(Node.class);
        makeThreadSafe(seed, true);
        seed.connect();
        expectLastCall().once();
        seed.send(EasyMock.<MembershipCommand>anyObject());
        expectLastCall().andReturn(new View("cluster", Sets.hash(new View.Member("discovered1", "localhost", 6000))));
        seed.disconnect();
        expectLastCall().once();
        //
        Node discoveredNode1 = createMock(Node.class);
        makeThreadSafe(discoveredNode1, true);
        discoveredNode1.connect();
        expectLastCall().once();
        discoveredNode1.disconnect();
        expectLastCall().once();
        discoveredNode1.send(EasyMock.<MembershipCommand>anyObject());
        expectLastCall().andReturn(new View("cluster", Sets.hash(new View.Member("discovered1", "localhost", 6000), new View.Member("discovered2", "localhost", 6001))));
        //
        Node discoveredNode2 = createMock(Node.class);
        makeThreadSafe(discoveredNode2, true);
        discoveredNode2.connect();
        expectLastCall().once();
        discoveredNode2.disconnect();
        expectLastCall().once();
        //
        RemoteNodeFactory nodeFactory = createMock(RemoteNodeFactory.class);
        makeThreadSafe(nodeFactory, true);
        nodeFactory.makeRemoteNode("localhost", 6000, "localhost:6000");
        expectLastCall().andReturn(seed).once();
        nodeFactory.makeRemoteNode("localhost", 6000, "discovered1");
        expectLastCall().andReturn(discoveredNode1).once();
        nodeFactory.makeRemoteNode("localhost", 6001, "discovered2");
        expectLastCall().andReturn(discoveredNode2).once();
        //
        Router router = createMock(Router.class);
        makeThreadSafe(router, true);
        router.addRouteTo(cluster, discoveredNode1);
        expectLastCall().once();
        router.addRouteTo(cluster, discoveredNode2);
        expectLastCall().once();

        replay(seed, discoveredNode1, discoveredNode2, nodeFactory, router);

        DefaultEnsemble ensemble = new DefaultEnsemble(router, nodeFactory);
        try {
            ensemble.join(cluster, "localhost:6000");
            Thread.sleep(3000);
            ensemble.update(cluster);
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            ensemble.shutdown();
            verify(seed, discoveredNode1, discoveredNode2, nodeFactory, router);
        }
    }

    @Test
    public void testJoinAndUpdateWithDisconnectedNode() throws Exception {
        Cluster cluster = new Cluster("cluster", false);

        Node seed = createMock(Node.class);
        makeThreadSafe(seed, true);
        seed.connect();
        expectLastCall().once();
        seed.send(EasyMock.<MembershipCommand>anyObject());
        expectLastCall().andReturn(new View("cluster", Sets.linked(new View.Member("discovered1", "localhost", 6000), new View.Member("discovered2", "localhost", 6001))));
        seed.disconnect();
        expectLastCall().once();
        //
        Node discoveredNode1 = createMock(Node.class);
        makeThreadSafe(discoveredNode1, true);
        discoveredNode1.getName();
        expectLastCall().andReturn("discovered1").once();
        discoveredNode1.connect();
        expectLastCall().once();
        discoveredNode1.disconnect();
        expectLastCall().once();
        discoveredNode1.send(EasyMock.<MembershipCommand>anyObject());
        expectLastCall().andReturn(new View("cluster", Sets.linked(new View.Member("discovered1", "localhost", 6000))));
        //
        Node discoveredNode2 = createMock(Node.class);
        makeThreadSafe(discoveredNode2, true);
        discoveredNode2.getName();
        expectLastCall().andReturn("discovered2").once();
        discoveredNode2.connect();
        expectLastCall().once();
        discoveredNode2.disconnect();
        expectLastCall().once();
        //
        RemoteNodeFactory nodeFactory = createMock(RemoteNodeFactory.class);
        makeThreadSafe(nodeFactory, true);
        nodeFactory.makeRemoteNode("localhost", 6000, "localhost:6000");
        expectLastCall().andReturn(seed).once();
        nodeFactory.makeRemoteNode("localhost", 6000, "discovered1");
        expectLastCall().andReturn(discoveredNode1).once();
        nodeFactory.makeRemoteNode("localhost", 6001, "discovered2");
        expectLastCall().andReturn(discoveredNode2).once();
        //
        Router router = createMock(Router.class);
        makeThreadSafe(router, true);
        router.addRouteTo(cluster, discoveredNode1);
        expectLastCall().once();
        router.addRouteTo(cluster, discoveredNode2);
        expectLastCall().once();
        router.removeRouteTo(cluster, discoveredNode2);
        expectLastCall().once();

        replay(seed, discoveredNode1, discoveredNode2, nodeFactory, router);

        DefaultEnsemble ensemble = new DefaultEnsemble(router, nodeFactory);
        try {
            ensemble.join(cluster, "localhost:6000");
            Thread.sleep(3000);
            ensemble.update(cluster);
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            ensemble.shutdown();
            verify(seed, discoveredNode1, discoveredNode2, nodeFactory, router);
        }
    }

    @Test
    public void testUpdateDoesNothingIfNoJoin() throws Exception {
        Cluster cluster = new Cluster("cluster", false);

        RemoteNodeFactory nodeFactory = createMock(RemoteNodeFactory.class);
        makeThreadSafe(nodeFactory, true);
        //
        Router router = createMock(Router.class);
        makeThreadSafe(router, true);

        replay(nodeFactory, router);

        DefaultEnsemble ensemble = new DefaultEnsemble(router, nodeFactory);
        try {
            ensemble.update(cluster);
        } finally {
            ensemble.shutdown();
            verify(nodeFactory, router);
        }
    }
}
