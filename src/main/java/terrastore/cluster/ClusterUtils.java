package terrastore.cluster;

import org.terracotta.cluster.ClusterNode;

/**
 * @author Sergio Bossa
 */
public class ClusterUtils {

    public static String getServerId(ClusterNode node) {
        return node.getId().replace("Client", "Server");
    }
}
