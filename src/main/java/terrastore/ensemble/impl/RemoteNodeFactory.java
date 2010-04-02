package terrastore.ensemble.impl;

import terrastore.communication.Node;

/**
 * @author Sergio Bossa
 */
public interface RemoteNodeFactory {

    public Node makeNode(String host, int port, String name);
}
