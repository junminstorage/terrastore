package terrastore.communication;

import terrastore.communication.local.LocalProcessor;

/**
 * @author Sergio Bossa
 */
public interface LocalNodeFactory {

    public Node makeLocalNode(String host, int port, String name, LocalProcessor processor);
}
