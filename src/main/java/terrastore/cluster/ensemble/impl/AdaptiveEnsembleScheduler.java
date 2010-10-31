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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.cluster.ensemble.EnsembleManager;
import terrastore.cluster.ensemble.EnsembleScheduler;
import terrastore.communication.Cluster;

/**
 * @author Amir Moulavi
 */
public class AdaptiveEnsembleScheduler implements EnsembleScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveEnsembleScheduler.class);
    private final FuzzyInferenceEngine fuzzyInferenceEngine;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture future;
    private boolean shutdown;
    private long discoveryInterval;
    private View prevView;

    public AdaptiveEnsembleScheduler(FuzzyInferenceEngine engine) {
        this.fuzzyInferenceEngine = engine;
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    @Override
    public final synchronized void schedule(final Cluster cluster, final EnsembleManager ensemble, final EnsembleConfiguration ensembleConfiguration) {
        if (!shutdown) {
            discoveryInterval = ensembleConfiguration.getDiscovery().getInterval();
            future = scheduler.scheduleWithFixedDelay(new Runnable() {

                @Override
                public void run() {
                    try {
                        View view = ensemble.update(cluster);
                        if (prevView != null) {
                            future.cancel(true);
                            long newEstimatedPeriodLength = fuzzyInferenceEngine.estimateNextPeriodLength(
                                    view.difference(prevView),
                                    discoveryInterval,
                                    ensembleConfiguration.getDiscovery());
                            reschedule(cluster,
                                    ensemble,
                                    ensembleConfiguration,
                                    newEstimatedPeriodLength);
                        }
                        prevView = view;
                    } catch (Exception ex) {
                        LOG.warn(ex.getMessage(), ex);
                    }
                }

            }, discoveryInterval, discoveryInterval, TimeUnit.MILLISECONDS);
            LOG.info("Scheduled discovery for cluster [{}], discoveryInterval {} ms", cluster, discoveryInterval);
        }
    }

    public final synchronized void reschedule(Cluster cluster, EnsembleManager ensemble, EnsembleConfiguration ensembleConfiguration, long estimatedPeriodLength) {
        ensembleConfiguration.getDiscovery().setInterval(estimatedPeriodLength);
        schedule(cluster, ensemble, ensembleConfiguration);
    }

    @Override
    public synchronized void shutdown() {
        shutdown = true;
        scheduler.shutdown();
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

}
