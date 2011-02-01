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
package terrastore.cluster.ensemble.impl;

import org.junit.Test;
import terrastore.cluster.ensemble.EnsembleManager;
import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.communication.Cluster;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class FixedEnsembleSchedulerTest {

    @Test
    public void testSchedule() throws Exception {
        Cluster cluster = new Cluster("cluster", false);

        View view = createMock(View.class);
        makeThreadSafe(view, true);
        EnsembleConfiguration.DiscoveryConfiguration discoveryConfiguration = createMock(EnsembleConfiguration.DiscoveryConfiguration.class);
        makeThreadSafe(discoveryConfiguration, true);
        discoveryConfiguration.getInterval();
        expectLastCall().andReturn(10000L).once();
        EnsembleConfiguration ensembleConfiguration = createMock(EnsembleConfiguration.class);
        makeThreadSafe(ensembleConfiguration, true);
        ensembleConfiguration.getDiscovery();
        expectLastCall().andReturn(discoveryConfiguration).once();
        EnsembleManager ensemble = createMock(EnsembleManager.class);
        makeThreadSafe(ensemble, true);
        ensemble.update(cluster);
        expectLastCall().andReturn(view).once();

        replay(discoveryConfiguration, ensembleConfiguration, ensemble);

        FixedEnsembleScheduler scheduler = new FixedEnsembleScheduler();
        try {
            scheduler.schedule(cluster, ensemble, ensembleConfiguration);
            Thread.sleep(1000);
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            scheduler.shutdown();
            verify(discoveryConfiguration, ensembleConfiguration, ensemble);
        }
    }

}
