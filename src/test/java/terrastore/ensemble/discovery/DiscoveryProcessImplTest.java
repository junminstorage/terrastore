package terrastore.ensemble.discovery;

import terrastore.ensemble.impl.JGroupsDiscoveryProcess;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import terrastore.ensemble.EnsembleConfiguration;
import terrastore.ensemble.EnsembleNodeFactory;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.communication.remote.RemoteProcessor;
import terrastore.router.Router;
import terrastore.store.Store;
import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

/**
 *
 * @author sergio
 */
public class DiscoveryProcessImplTest {

    public DiscoveryProcessImplTest() {
    }

    @Test
    public void testDiscoveryProcess() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();

        Router router1 = createMock(Router.class);
        makeThreadSafe(router1, true);
        router1.addRouteTo(eq(new Cluster("cluster2", false)), EasyMock.<Node>anyObject());
        expectLastCall().once();
        //router1.removeRouteTo(eq(new Cluster("cluster2", false)), EasyMock.<Node>anyObject());
        //expectLastCall().once();
        Router router2 = createMock(Router.class);
        makeThreadSafe(router2, true);
        router2.addRouteTo(eq(new Cluster("cluster1", false)), EasyMock.<Node>anyObject());
        expectLastCall().once();
        router2.removeRouteTo(eq(new Cluster("cluster1", false)), EasyMock.<Node>anyObject());
        expectLastCall().once();
        Node nodeToCluster1 = createMock(Node.class);
        makeThreadSafe(nodeToCluster1, true);
        nodeToCluster1.connect();
        expectLastCall().once();
        nodeToCluster1.disconnect();
        expectLastCall().once();
        Node nodeToCluster2 = createMock(Node.class);
        makeThreadSafe(nodeToCluster2, true);
        nodeToCluster2.connect();
        expectLastCall().once();

        final EnsembleNodeFactory nodeFactory = createMock(EnsembleNodeFactory.class);
        makeThreadSafe(nodeFactory, true);
        nodeFactory.makeNode("127.0.0.1", 6000);
        expectLastCall().andReturn(nodeToCluster1);
        nodeFactory.makeNode("127.0.0.1", 6001);
        expectLastCall().andReturn(nodeToCluster2);

        replay(router1, router2, nodeToCluster1, nodeToCluster2, nodeFactory);

        final EnsembleConfiguration conf1 = new EnsembleConfiguration();
        conf1.setClusterName("cluster1");
        conf1.setEnsembleName("ensemble");
        conf1.setDiscoveryHost("127.0.0.1");
        conf1.setDiscoveryPort("8000");
        conf1.setInitialHosts("127.0.0.1[8000],127.0.0.1[8001]");
        conf1.setClusters(Arrays.asList("cluster1", "cluster2"));

        final EnsembleConfiguration conf2 = new EnsembleConfiguration();
        conf2.setClusterName("cluster2");
        conf2.setEnsembleName("ensemble");
        conf2.setDiscoveryHost("127.0.0.1");
        conf2.setDiscoveryPort("8001");
        conf2.setInitialHosts("127.0.0.1[8000],127.0.0.1[8001]");
        conf2.setClusters(Arrays.asList("cluster1", "cluster2"));

        final JGroupsDiscoveryProcess process1 = new JGroupsDiscoveryProcess(router1);

        final JGroupsDiscoveryProcess process2 = new JGroupsDiscoveryProcess(router2);

        executor.submit(new Runnable() {

            @Override
            public void run() {
                process1.start("127.0.0.1", 6000, conf1, nodeFactory);
            }
        });
        Thread.sleep(3000);
        executor.submit(new Runnable() {

            @Override
            public void run() {
                process2.start("127.0.0.1", 6001, conf2, nodeFactory);
            }
        });

        Thread.sleep(5000);

        process1.stop();
        //process2.stop();

        Thread.sleep(5000);

        verify(router1, router2, nodeToCluster1, nodeToCluster2, nodeFactory);
    }
}