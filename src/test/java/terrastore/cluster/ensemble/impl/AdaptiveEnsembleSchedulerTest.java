package terrastore.cluster.ensemble.impl;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.makeThreadSafe;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;

import org.junit.Test;

import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.cluster.ensemble.EnsembleManager;
import terrastore.communication.Cluster;

/**
 * 
 * @author Amir Moulavi
 *
 */

public class AdaptiveEnsembleSchedulerTest {
	   
	@Test
	public void first_time_schedule() throws Exception {
		Cluster cluster = new Cluster("cluster", false);

        EnsembleConfiguration configuration = createMock(EnsembleConfiguration.class);
        makeThreadSafe(configuration, true);
        configuration.getDiscoveryInterval();
        expectLastCall().andReturn(10000).once();
        EnsembleManager ensemble = createMock(EnsembleManager.class);
        makeThreadSafe(ensemble, true);
        ensemble.update(cluster);
        expectLastCall().andReturn(null).once();

        replay(configuration, ensemble);

        AdaptiveEnsembleScheduler scheduler = new AdaptiveEnsembleScheduler();
        try {
            scheduler.schedule(cluster, ensemble, configuration);
            Thread.sleep(1000);
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            scheduler.shutdown();
            verify(configuration, ensemble);
        }
    }
}
