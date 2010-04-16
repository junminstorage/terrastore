package terrastore.communication;

/**
 * @author Sergio Bossa
 */
public interface RemoteNodeFactory {

    public Node makeRemoteNode(String host, int port, String name);

    public Node makeRemoteNode(String host, int port, String name, int maxFrameLength, long nodeTimeout);
}
