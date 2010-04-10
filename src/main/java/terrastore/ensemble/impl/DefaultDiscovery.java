package terrastore.ensemble.impl;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.ensemble.impl.View.Member;
import terrastore.communication.protocol.MembershipCommand;
import terrastore.ensemble.Discovery;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;

/**
 * Default {@link terrastore.ensemble.Discovery} implementation.
 *
 * @author Sergio Bossa
 */
public class DefaultDiscovery implements Discovery {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDiscovery.class);
    //
    private final Router router;
    private final RemoteNodeFactory nodeFactory;
    private final ConcurrentMap<Cluster, Node> bootstrapNodes;
    private final ConcurrentMap<Cluster, List<Node>> perClusterNodes;
    private final ConcurrentMap<Cluster, View> perClusterViews;
    private volatile ScheduledExecutorService scheduler;
    private volatile boolean scheduled;
    private volatile boolean shutdown;

    public DefaultDiscovery(Router router, RemoteNodeFactory nodeFactory) {
        this.router = router;
        this.nodeFactory = nodeFactory;
        this.bootstrapNodes = new ConcurrentHashMap<Cluster, Node>();
        this.perClusterNodes = new ConcurrentHashMap<Cluster, List<Node>>();
        this.perClusterViews = new ConcurrentHashMap<Cluster, View>();
    }

    @Override
    public synchronized void join(Cluster cluster, String seed) throws MissingRouteException, ProcessingException {
        if (!cluster.isLocal()) {
            String[] hostPortPair = seed.split(":");
            bootstrapNodes.put(cluster, nodeFactory.makeNode(hostPortPair[0], Integer.parseInt(hostPortPair[1]), seed));
            update(cluster);
        } else {
            throw new IllegalArgumentException("No need to join local cluster: " + cluster);
        }
    }

    @Override
    public synchronized void schedule(long delay, long interval, TimeUnit timeUnit) {
        if (!scheduled && !shutdown) {
            scheduler = Executors.newScheduledThreadPool(bootstrapNodes.size());
            for (final Cluster cluster : bootstrapNodes.keySet()) {
                LOG.info("Scheduling discovery for cluster {}", cluster);
                scheduler.scheduleWithFixedDelay(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            // Update doesn't need to be synchronized because there's one single thread per-cluster,
                            // and backing maps are concurrent.
                            update(cluster);
                        } catch (Exception ex) {
                            LOG.warn(ex.getMessage(), ex);
                        }
                    }
                }, delay, interval, timeUnit);
            }
            scheduled = true;
        }
    }

    @Override
    public synchronized void shutdown() {
        shutdown = true;
        cancelScheduler();
        disconnectAllClustersNodes();
    }

    private void cancelScheduler() {
        scheduler.shutdownNow();
        scheduled = false;
    }

    private void update(Cluster cluster) throws MissingRouteException, ProcessingException {
        try {
            List<Node> nodes = perClusterNodes.get(cluster);
            if (nodes == null || nodes.isEmpty()) {
                LOG.debug("Bootstrapping discovery for cluster {}", cluster);
                Node bootstrap = null;
                try {
                    bootstrap = bootstrapNodes.get(cluster);
                    bootstrap.connect();
                    View view = requestMembership(cluster, Arrays.asList(bootstrap));
                    calculateView(cluster, view);
                } finally {
                    bootstrap.disconnect();
                }
            } else {
                LOG.debug("Updating cluster view for {}", cluster);
                View view = requestMembership(cluster, nodes);
                calculateView(cluster, view);
            }
        } catch (Exception ex) {
            LOG.info("Error updating membership information for cluster {}", cluster);
            LOG.debug(ex.getMessage(), ex);
        }
    }

    private View requestMembership(Cluster cluster, List<Node> contactNodes) throws MissingRouteException, ProcessingException {
        Iterator<Node> nodeIterator = contactNodes.iterator();
        boolean successful = false;
        View view = null;
        while (!successful && nodeIterator.hasNext()) {
            Node node = null;
            try {
                node = nodeIterator.next();
                view = node.<View>send(new MembershipCommand());
                successful = true;
                LOG.debug("Updated cluster view from node {}:{}", cluster, node);
            } catch (Exception ex) {
                LOG.warn("Failed to contact node {}:{} for updating cluster view!", cluster, node);
                router.removeRouteTo(cluster, node);
                node.disconnect();
                nodeIterator.remove();
                LOG.info("Disconnected remote node {}:{}", cluster, node);
            }
        }
        if (successful) {
            return view;
        } else {
            throw new MissingRouteException(new ErrorMessage(ErrorMessage.UNAVAILABLE_ERROR_CODE, "No route to cluster " + cluster));
        }
    }

    private void calculateView(Cluster cluster, View updatedView) {
        List<Node> currentNodes = perClusterNodes.get(cluster);
        if (currentNodes == null) {
            currentNodes = new LinkedList<Node>();
            perClusterNodes.put(cluster, currentNodes);
        }
        //
        View currentView = perClusterViews.get(cluster);
        if (currentView != null) {
            LOG.debug("Current view for cluster {} : {}", cluster, currentView);
            LOG.debug("Updated view for cluster {} : {}", cluster, updatedView);
            Set<View.Member> leavingMembers = Sets.difference(currentView.getMembers(), updatedView.getMembers());
            Set<View.Member> joiningMembers = Sets.difference(updatedView.getMembers(), currentView.getMembers());
            for (View.Member member : leavingMembers) {
                Node node = findNode(currentNodes, member);
                if (node != null) {
                    router.removeRouteTo(cluster, node);
                    node.disconnect();
                    currentNodes.remove(node);
                    LOG.info("Disconnected remote node {}:{}", cluster, node);
                }
            }
            for (View.Member member : joiningMembers) {
                Node node = nodeFactory.makeNode(member.getHost(), member.getPort(), member.getName());
                router.addRouteTo(cluster, node);
                node.connect();
                currentNodes.add(node);
                LOG.info("Joining remote node {}:{}", cluster, node);
            }
        } else {
            LOG.debug("No current view for cluster {}", cluster);
            LOG.debug("Updated view for cluster {} :  {}", cluster, updatedView);
            for (View.Member member : updatedView.getMembers()) {
                Node node = nodeFactory.makeNode(member.getHost(), member.getPort(), member.getName());
                router.addRouteTo(cluster, node);
                node.connect();
                currentNodes.add(node);
                LOG.info("Joining remote node {}:{}", cluster, node);
            }
        }
        perClusterViews.put(cluster, updatedView);
    }

    private Node findNode(List<Node> nodes, Member member) {
        try {
            return Iterables.find(nodes, new NodeFinder(member.getName()));
        } catch (NoSuchElementException ex) {
            return null;
        }
    }

    private void disconnectAllClustersNodes() {
        for (List<Node> nodes : perClusterNodes.values()) {
            for (Node node : nodes) {
                try {
                    node.disconnect();
                } catch (Exception ex) {
                }
            }
        }
    }

    private static class NodeFinder implements Predicate<Node> {

        private final String name;

        public NodeFinder(String name) {
            this.name = name;
        }

        @Override
        public boolean apply(Node node) {
            return node.getName().equals(name);
        }
    }
}
