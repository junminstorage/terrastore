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
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.communication.Cluster;
import terrastore.cluster.ensemble.EnsembleManager;
import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.cluster.ensemble.EnsembleScheduler;

/**
 * Default {@link terrastore.ensemble.EnsembleScheduler} implementation, scheduling ensemble updates at fixed time intervals.
 * Time information is provided by the {@link terrastore.cluster.ensemble.EnsembleConfiguration} passed at the
 * {@link #schedule(Cluster, EnsembleManager, EnsembleConfiguration)} method.
 *
 * @author Sergio Bossa
 */
public class FixedEnsembleScheduler implements EnsembleScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(FixedEnsembleScheduler.class);
    //
    private final ScheduledExecutorService scheduler;
    private volatile boolean shutdown;

    public FixedEnsembleScheduler() {
        this.scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public final synchronized void schedule(final Cluster cluster, final EnsembleManager ensemble, final EnsembleConfiguration ensembleConfiguration) {
        if (!shutdown) {
            LOG.info("Scheduling discovery for cluster {}", cluster);
            scheduler.scheduleWithFixedDelay(new Runnable() {

                @Override
                public void run() {
                    try {
                        ensemble.update(cluster);
                    } catch (Exception ex) {
                        LOG.warn(ex.getMessage(), ex);
                    }
                }

            }, 0, ensembleConfiguration.getDiscoveryInterval(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public synchronized void shutdown() {
        shutdown = true;
        scheduler.shutdown();
    }

}
