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
