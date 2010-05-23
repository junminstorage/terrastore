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
package terrastore.cluster.ensemble;

import java.util.concurrent.TimeUnit;
import terrastore.communication.Cluster;
import terrastore.communication.ProcessingException;
import terrastore.communication.RemoteNodeFactory;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;

/**
 * Ensemble interface to join other clusters and schedule membership updates.
 *
 * @author Sergio Bossa
 */
public interface Ensemble {

    /**
     * Join the given {@link terrastore.communication.Cluster}, using the given seed host specified as an <i>host</i>:<i>port</i> string.
     *
     * @param cluster The cluster to join.
     * @param seed The seed host.
     * @throws MissingRouteException If unable to establish a route to the given cluster and host.
     * @throws ProcessingException If unable to process membership messages from the given host.
     */
    public void join(Cluster cluster, String seed) throws MissingRouteException, ProcessingException;

    /**
     * Update the given {@link terrastore.communication.Cluster} membership.
     *
     * @param cluster The cluster to update.
     * @throws MissingRouteException If unable to establish a route to the given cluster.
     * @throws ProcessingException If unable to process membership messages from the given host.
     */
    public void update(Cluster cluster) throws MissingRouteException, ProcessingException;

    /**
     * Schedule membership updates toward joined hosts.<br>
     * Please note that membership updates are only exchanged between previously joined hosts.
     *
     * @param delay Delay time for the first membership update.
     * @param interval Interval between membership updates.
     * @param timeUnit Time unit for delay and interval.
     */
    public void schedule(long delay, long interval, TimeUnit timeUnit);

    /**
     * Shutdown by cancelling scheduled updates and disconnecting discovered nodes.
     */
    public void shutdown();

    /**
     * Get the {@link terrastore.router.Router} used to manage cluster routes.
     *
     * @return The {@link terrastore.router.Router} instance.
     */
    public Router getRouter();

    /**
     * Get the {@link terrastore.communication.RemoteNodeFactory} used to create remote nodes.
     *
     * @return The {@link terrastore.communication.RemoteNodeFactory} instance.
     */
    public RemoteNodeFactory getRemoteNodeFactory();
}
