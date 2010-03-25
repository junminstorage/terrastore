package terrastore.ensemble;

import terrastore.ensemble.EnsembleConfiguration;
import terrastore.ensemble.EnsembleConfiguration;
import terrastore.ensemble.EnsembleNodeFactory;
import terrastore.ensemble.EnsembleNodeFactory;
import terrastore.router.Router;

/**
 * @author Sergio Bossa
 */
public interface DiscoveryProcess {

    public void start(String host, int port, EnsembleConfiguration ensembleConfiguration, EnsembleNodeFactory ensembleNodeFactory);

    public void stop();

    public Router getRouter();
}
