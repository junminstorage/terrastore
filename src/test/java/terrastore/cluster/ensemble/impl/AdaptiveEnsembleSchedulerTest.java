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

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.makeThreadSafe;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.cluster.ensemble.EnsembleManager;
import terrastore.cluster.ensemble.EnsembleConfiguration.DiscoveryConfiguration;
import terrastore.communication.Cluster;

/**
 * @author Amir Moulavi
 * @author Sergio Bossa
 */
public class AdaptiveEnsembleSchedulerTest {

    private Cluster cluster;

    @Before
    public void set_up() {
        cluster = new Cluster("cluster", false);
    }
    
    @Test
    public void scheduler_starts_as_expected() throws Exception {
        View view = createMock(View.class);
        makeThreadSafe(view, true);
        //
        DiscoveryConfiguration discoveryConfiguration = createMock(EnsembleConfiguration.DiscoveryConfiguration.class);
        makeThreadSafe(discoveryConfiguration, true);
        discoveryConfiguration.getInterval();
        expectLastCall().andReturn(1000L).anyTimes();
        //
        EnsembleConfiguration ensembleConfiguration = createMock(EnsembleConfiguration.class);
        makeThreadSafe(ensembleConfiguration, true);
        ensembleConfiguration.getDiscovery();
        expectLastCall().andReturn(discoveryConfiguration).anyTimes();
        //
        FuzzyInferenceEngine fuzzy = createMock(FuzzyInferenceEngine.class);
        makeThreadSafe(fuzzy, true);
        //
        EnsembleManager ensemble = createMock(EnsembleManager.class);
        makeThreadSafe(ensemble, true);
        ensemble.update(cluster);
        expectLastCall().andReturn(view).once();

        replay(view, fuzzy, discoveryConfiguration, ensembleConfiguration, ensemble);

        AdaptiveEnsembleScheduler scheduler = new AdaptiveEnsembleScheduler(fuzzy);

        try {
            scheduler.schedule(cluster, ensemble, ensembleConfiguration);
            Thread.sleep(3000);
            Assert.assertFalse(
                    "Scheduler should not be terminated but it is terminated. Have you changed the delay back to 0?!",
                    scheduler.getScheduler().isTerminated());
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            scheduler.shutdown();
            verify(view, fuzzy, discoveryConfiguration, ensembleConfiguration, ensemble);
        }
    }

    @Test
    public void scheduler_is_rescheduled_as_expected() throws Exception {
        View view1 = createMock(View.class);
        makeThreadSafe(view1, true);
        View view2 = createMock(View.class);
        makeThreadSafe(view2, true);
        view2.percentageOfChange(view1);
        expectLastCall().andReturn(0).once();
        //
        DiscoveryConfiguration discoveryConfiguration = createMock(EnsembleConfiguration.DiscoveryConfiguration.class);
        makeThreadSafe(discoveryConfiguration, true);
        discoveryConfiguration.getInterval();
        expectLastCall().andReturn(1000L).anyTimes();
        //
        EnsembleConfiguration ensembleConfiguration = createMock(EnsembleConfiguration.class);
        makeThreadSafe(ensembleConfiguration, true);
        ensembleConfiguration.getDiscovery();
        expectLastCall().andReturn(discoveryConfiguration).anyTimes();
        //
        FuzzyInferenceEngine fuzzy = createMock(FuzzyInferenceEngine.class);
        makeThreadSafe(fuzzy, true);
        fuzzy.estimateNextPeriodLength(0, 1000L, discoveryConfiguration);
        expectLastCall().andReturn(10000L).once();
        //
        EnsembleManager ensemble = createMock(EnsembleManager.class);
        makeThreadSafe(ensemble, true);
        ensemble.update(cluster);
        expectLastCall().andReturn(view1).once().andReturn(view2).once();

        replay(view1, view2, fuzzy, discoveryConfiguration, ensembleConfiguration, ensemble);

        AdaptiveEnsembleScheduler scheduler = new AdaptiveEnsembleScheduler(fuzzy);

        try {
            scheduler.schedule(cluster, ensemble, ensembleConfiguration);
            Thread.sleep(5000);
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            scheduler.shutdown();
            verify(view1, view2, fuzzy, discoveryConfiguration, ensembleConfiguration, ensemble);
        }
    }

    @Test
    public void scheduler_is_rescheduled_as_expected_with_null_view() throws Exception {
        View view = createMock(View.class);
        makeThreadSafe(view, true);
        //
        DiscoveryConfiguration discoveryConfiguration = createMock(EnsembleConfiguration.DiscoveryConfiguration.class);
        makeThreadSafe(discoveryConfiguration, true);
        discoveryConfiguration.getInterval();
        expectLastCall().andReturn(1000L).anyTimes();
        //
        EnsembleConfiguration ensembleConfiguration = createMock(EnsembleConfiguration.class);
        makeThreadSafe(ensembleConfiguration, true);
        ensembleConfiguration.getDiscovery();
        expectLastCall().andReturn(discoveryConfiguration).anyTimes();
        //
        FuzzyInferenceEngine fuzzy = createMock(FuzzyInferenceEngine.class);
        makeThreadSafe(fuzzy, true);
        fuzzy.estimateNextPeriodLength(100, 1000L, discoveryConfiguration);
        expectLastCall().andReturn(10000L).once();
        //
        EnsembleManager ensemble = createMock(EnsembleManager.class);
        makeThreadSafe(ensemble, true);
        ensemble.update(cluster);
        expectLastCall().andReturn(view).once().andReturn(null).once();

        replay(view, fuzzy, discoveryConfiguration, ensembleConfiguration, ensemble);

        AdaptiveEnsembleScheduler scheduler = new AdaptiveEnsembleScheduler(fuzzy);

        try {
            scheduler.schedule(cluster, ensemble, ensembleConfiguration);
            Thread.sleep(5000);
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            scheduler.shutdown();
            verify(view, fuzzy, discoveryConfiguration, ensembleConfiguration, ensemble);
        }
    }
}
