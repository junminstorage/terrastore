package terrastore.ensemble;

import terrastore.communication.Cluster;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;

/**
 * @author Sergio Bossa
 */
public interface Discovery {

    void join(Cluster cluster, String seed) throws MissingRouteException, ProcessingException;

    void update() throws MissingRouteException, ProcessingException;

}
