package terrastore.ensemble;

import java.util.LinkedList;
import java.util.List;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.Command;

/**
 * @author Sergio Bossa
 */
public class ProxyContactNode implements Node {

    private final List<Node> nodes = new LinkedList<Node>();

    public void addNode(Node node) {
        nodes.add(node);
    }

    public void removeNode(Node node) {
        nodes.remove(node);
    }

    @Override
    public <R> R send(Command<R> command) throws ProcessingException {
        return nodes.get(0).send(command);
    }

    @Override
    public void connect() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void disconnect() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
