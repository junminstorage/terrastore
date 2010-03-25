package terrastore.ensemble.impl;

import terrastore.ensemble.DiscoveryProcess;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import terrastore.ensemble.EnsembleConfiguration;
import terrastore.ensemble.EnsembleNodeFactory;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.router.Router;

/**
 * @author Sergio Bossa
 */
public class JGroupsDiscoveryProcess extends ReceiverAdapter implements RequestHandler, DiscoveryProcess {

    // WARN REMOVE SOUTs!

    private final ConcurrentMap<Address, Cluster> clusters = new ConcurrentHashMap<Address, Cluster>();
    private final ConcurrentMap<Address, Node> nodes = new ConcurrentHashMap<Address, Node>();
    //
    private final Router router;
    //
    private volatile String host;
    private volatile int port;
    private volatile EnsembleConfiguration configuration;
    private volatile EnsembleNodeFactory nodeFactory;
    private volatile Channel channel;
    private volatile MessageDispatcher dispatcher;
    private volatile View lastView;

    public JGroupsDiscoveryProcess(Router router) {
        this.router = router;
    }

    @Override
    public void start(String host, int port, EnsembleConfiguration ensembleConfiguration, EnsembleNodeFactory ensembleNodeFactory) {
        this.host = host;
        this.port = port;
        this.configuration = ensembleConfiguration;
        this.nodeFactory = ensembleNodeFactory;
        try {
            setupDiscoveryHost(configuration.getDiscoveryHost(), configuration.getDiscoveryPort());
            setupInitialHosts(configuration.getInitialHosts());
            setupIpStack();
            channel = new JChannel(Thread.currentThread().getContextClassLoader().getResource("tcp.xml"));
            dispatcher = new MessageDispatcher(channel, this, this, this);
            channel.connect(configuration.getEnsembleName());
        } catch (Exception ex) {
        }
    }

    @Override
    public void stop() {
        try {
            channel.disconnect();
        } catch (Exception ex) {
        }
    }

    @Override
    public Router getRouter() {
        return router;
    }

    @Override
    public void viewAccepted(View newView) {
        System.out.println(configuration.getClusterName() + " ... " + newView.printDetails());
        try {
            if (lastView != null) {
                for (Address address : newView.getMembers()) {
                    if (!lastView.containsMember(address) && !address.equals(channel.getAddress())) {
                        System.out.println("NodeInfoRequest to " + address);
                        NodeInfoResponse response = (NodeInfoResponse) dispatcher.sendMessage(new Message(address, channel.getAddress(), new NodeInfoRequest()), RequestOptions.SYNC);
                        System.out.println(response);
                        handleNodeInfoResponse(address, response);
                    }
                }
                for (Address address : lastView.getMembers()) {
                    if (!newView.containsMember(address)) {
                        Cluster cluster = clusters.remove(address);
                        Node removed = nodes.remove(address);
                        removed.disconnect();
                        router.removeRouteTo(cluster, removed);
                    }
                }
            } else {
                for (Address address : newView.getMembers()) {
                    if (!address.equals(channel.getAddress())) {
                        System.out.println("NodeInfoRequest to " + address);
                        NodeInfoResponse response = (NodeInfoResponse) dispatcher.sendMessage(new Message(address, channel.getAddress(), new NodeInfoRequest()), RequestOptions.SYNC);
                        handleNodeInfoResponse(address, response);
                    }
                }
            }
            lastView = newView;
        } catch (Exception ex) {
        }
    }

    @Override
    public Object handle(Message message) {
        try {
            System.out.println("Message from: " + message.getSrc());
            Object payload = message.getObject();
            if (payload instanceof NodeInfoRequest) {
                System.out.println("NodeInfoResponse to " + message.getSrc());
                return new NodeInfoResponse(configuration.getClusterName(), host, port);
            } else {
                throw new IllegalStateException("Unexpected message payload of type: " + payload.getClass());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    private void setupDiscoveryHost(String discoveryHost, String discoveryPort) {
        System.setProperty("jgroups.bind_addr", discoveryHost);
        System.setProperty("jgroups.bind_port", discoveryPort);
    }

    private void setupInitialHosts(String hosts) {
        System.setProperty("jgroups.tcpping.initial_hosts", hosts);
    }

    private void setupIpStack() {
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    private void handleNodeInfoResponse(Address source, NodeInfoResponse response) {
        System.out.println("NodeInfoResponse from " + source);
        if (!configuration.getClusterName().equals(response.getCluster()) && configuration.getClusters().contains(response.getCluster())) {
            Cluster cluster = new Cluster(response.getCluster(), false);
            Node node = nodeFactory.makeNode(response.getHost(), response.getPort());
            node.connect();
            clusters.put(source, cluster);
            nodes.put(source, node);
            System.out.println("Route to " + cluster);
            router.addRouteTo(cluster, node);
        } else if (!configuration.getClusters().contains(response.getCluster())) {
            throw new IllegalStateException("Wrong cluster name: " + response.getCluster());
        }
    }

    private static class NodeInfoRequest implements Serializable {
    }

    private static class NodeInfoResponse implements Serializable {

        private final String cluster;
        private final String host;
        private final int port;

        public NodeInfoResponse(String cluster, String host, int port) {
            this.cluster = cluster;
            this.host = host;
            this.port = port;
        }

        public String getCluster() {
            return cluster;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }
}
