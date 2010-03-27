package terrastore.ensemble.impl;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Map;
import java.util.Set;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.ensemble.DiscoveryProcess;
import terrastore.ensemble.EnsembleConfiguration;
import terrastore.ensemble.EnsembleNodeFactory;
import terrastore.router.Router;

/**
 * @author Sergio Bossa
 */
public class StaticDiscoveryProcess implements DiscoveryProcess {

    private final Router router;

    public StaticDiscoveryProcess(Router router) {
        this.router = router;
    }

    @Override
    public void start(EnsembleConfiguration ensembleConfiguration, EnsembleNodeFactory ensembleNodeFactory) {
        setupHosts(ensembleConfiguration, ensembleNodeFactory);
    }

    @Override
    public void stop() {
    }

    @Override
    public Router getRouter() {
        return router;
    }

    private void setupHosts(EnsembleConfiguration ensembleConfiguration, EnsembleNodeFactory ensembleNodeFactory) {
        Set<Cluster> clusters = router.getClusters();
        Map<String, List<String>> clustersToHosts = ensembleConfiguration.getStaticDiscovery().getHosts();
        for (Map.Entry<String, List<String>> entry : clustersToHosts.entrySet()) {
            String clusterName = entry.getKey();
            List<String> nodesList = entry.getValue();
            Cluster cluster = getClusterByName(clusters, clusterName);
            if (cluster != null && !cluster.isLocal()) {
                for (String nodeString : nodesList) {
                    String[] nameHostPort = nodeString.split(":");
                    Node node = ensembleNodeFactory.makeNode(nameHostPort[0], nameHostPort[1], Integer.parseInt(nameHostPort[2]));
                    router.addRouteTo(cluster, node);
                }
            }
            // FIXME: what if cluster doesn't exist or is local?
        }
    }

    private Cluster getClusterByName(final Set<Cluster> clusters, final String name) {
        try {
            return Iterables.find(clusters, new Predicate<Cluster>() {

                @Override
                public boolean apply(Cluster candidate) {
                    return candidate.getName().equals(name);
                }
            });
        } catch (Exception ex) {
            return null;
        }
    }
}
