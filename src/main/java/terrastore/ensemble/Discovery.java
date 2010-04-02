package terrastore.ensemble;

import java.util.concurrent.TimeUnit;
import terrastore.communication.Cluster;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;

/**
 * Discovery process to join ensemble clusters and schedule membership updates.
 *
 * @author Sergio Bossa
 */
public interface Discovery {

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
     * Schedule membership updates toward joined hosts.<br>
     * Please note that membership updates are only exchanged between previously joined hosts.
     *
     * @param delay Delay time for the first membership update.
     * @param interval Interval between membership updates.
     * @param timeUnit Time unit for delay and interval.
     */
    public void schedule(long delay, long interval, TimeUnit timeUnit);

    /**
     * Cancel previously scheduled updates.
     */
    public void cancel();
}
