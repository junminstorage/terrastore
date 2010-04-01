package terrastore.ensemble;

import java.util.concurrent.TimeUnit;
import terrastore.communication.Cluster;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;

/**
 * @author Sergio Bossa
 */
public interface Discovery {

    public void join(Cluster cluster, String seed) throws MissingRouteException, ProcessingException;

    public void schedule(long delay, long interval, TimeUnit timeUnit);

    public void cancel();
}
