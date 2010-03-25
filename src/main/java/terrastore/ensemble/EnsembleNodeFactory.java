package terrastore.ensemble;

import terrastore.communication.Node;

/**
 * @author Sergio Bossa
 */
public interface EnsembleNodeFactory {

    public Node makeNode(String host, int port);
}
