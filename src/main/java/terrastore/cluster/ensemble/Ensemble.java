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
