package terrastore.ensemble;

import terrastore.router.Router;

/**
 * @author Sergio Bossa
 */
public interface DiscoveryProcess {

    public void start(EnsembleConfiguration ensembleConfiguration, EnsembleNodeFactory ensembleNodeFactory);

    public void stop();

    public Router getRouter();
}
