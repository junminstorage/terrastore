package terrastore.communication.protocol;

import java.util.HashSet;
import java.util.Set;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.cluster.ensemble.impl.View;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;

/**
 * @author Sergio Bossa
 */
public class MembershipCommand extends AbstractCommand<View> {

    @Override
    public View executeOn(Router router) throws MissingRouteException, ProcessingException {
        Cluster localCluster = getLocalCluster(router);
        Set<Node> nodes = router.clusterRoute(localCluster);
        Set<View.Member> viewMembers = new HashSet<View.Member>();
        for (Node node : nodes) {
            viewMembers.add(new View.Member(node.getName(), node.getHost(), node.getPort()));
        }
        return new View(localCluster.getName(), viewMembers);
    }

    @Override
    public View executeOn(Store store) throws StoreOperationException {
        throw new UnsupportedOperationException("MembershipCommand cannot be executed on a Store!");
    }

    private Cluster getLocalCluster(Router router) {
        Cluster locaCluster = null;
        for (Cluster cluster : router.getClusters()) {
            if (cluster.isLocal()) {
                locaCluster = cluster;
                break;
            }
        }
        return locaCluster;
    }
}
