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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.cluster.ensemble.EnsembleManager;
import terrastore.cluster.ensemble.EnsembleConfiguration.DiscoveryConfiguration;
import terrastore.communication.Cluster;

/**
 * @author Amir Moulavi
 */
public class AdaptiveEnsembleSchedulerTest {

    private Cluster cluster;
    private FuzzyInferenceEngine fuzzy;
    private View view;
    private DiscoveryConfiguration discoveryConfiguration;
    private EnsembleConfiguration ensembleConfiguration;
    private EnsembleManager ensemble;
    private AdaptiveEnsembleScheduler scheduler;

    @Before
    public void set_up() {
        cluster = new Cluster("cluster", false);
        scheduler = new AdaptiveEnsembleScheduler(fuzzy);
    }
    
    @Test
    public void scheduler_works_as_expected() throws Exception {
        given(
                fuzzy_inference_engine(), 
                view(), 
                discovery_configuration(),
                ensemble_configuration(),
                ensemble_manager()
             );
        
        when_all_objects_are_constructed_and_called_properly();

        then_scheduler_works_as_expected();
    }

    @Test
    public void scheduler_starts_with_a_delay() throws Exception {
        given(
                fuzzy_inference_engine(), 
                view(), 
                discovery_configuration(),
                ensemble_configuration()
             );
        
        when_all_objects_are_constructed_and_partially_called();
        
        then_scheduler_should_not_be_terminated_after_one_second();
    }
    
    private void when_all_objects_are_constructed_and_partially_called() {
        replay(fuzzy, discoveryConfiguration, ensembleConfiguration);
    }

    private void when_all_objects_are_constructed_and_called_properly() throws Exception {
        replay(fuzzy, discoveryConfiguration, ensembleConfiguration, ensemble);
    }

    private void then_scheduler_works_as_expected() {
        try {
            scheduler.schedule(cluster, ensemble, ensembleConfiguration);
            Thread.sleep(4000);
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            scheduler.shutdown();
            verify(fuzzy, discoveryConfiguration, ensembleConfiguration);
        }
    }

    private void then_scheduler_should_not_be_terminated_after_one_second() {
        try {
            scheduler.schedule(cluster, ensemble, ensembleConfiguration);
            Thread.sleep(1000);
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            Assert.assertFalse("Scheduler should not be terminated but it is terminated. Have you changed the delay back to 0?!", scheduler.getScheduler().isTerminated());
            scheduler.shutdown();
            verify(fuzzy, discoveryConfiguration, ensembleConfiguration);
        }
    }


    private void given(Object... object) {
    }

    private EnsembleManager ensemble_manager() throws Exception {
        EnsembleManager ensemble = createMock(EnsembleManager.class);
        makeThreadSafe(ensemble, true);
        this.ensemble = ensemble;
        ensemble.update(cluster);
        expectLastCall().andReturn(view).once();
        return ensemble;
    }

    private EnsembleConfiguration ensemble_configuration() {
        EnsembleConfiguration ensembleConfiguration = createMock(EnsembleConfiguration.class);
        makeThreadSafe(ensembleConfiguration, true);
        this.ensembleConfiguration = ensembleConfiguration;
        ensembleConfiguration.getDiscovery();
        expectLastCall().andReturn(discoveryConfiguration).once();
        return ensembleConfiguration;
    }

    private EnsembleConfiguration.DiscoveryConfiguration discovery_configuration() {
        EnsembleConfiguration.DiscoveryConfiguration discoveryConfiguration = createMock(EnsembleConfiguration.DiscoveryConfiguration.class);
        makeThreadSafe(discoveryConfiguration, true);
        this.discoveryConfiguration = discoveryConfiguration;
        discoveryConfiguration.getInterval();
        expectLastCall().andReturn(3000L).once();
        return discoveryConfiguration;
    }

    private View view() {
        View view = createMock(View.class);
        makeThreadSafe(view, true);
        this.view = view;
        return view;
    }

    private FuzzyInferenceEngine fuzzy_inference_engine() {
        FuzzyInferenceEngine fuzzy = createMock(FuzzyInferenceEngine.class);
        makeThreadSafe(fuzzy, true);
        this.fuzzy = fuzzy;
        return fuzzy;
    }

}
