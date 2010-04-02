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
    private final ConcurrentMap<Cluster, List<Node>> perClusterNodes;
    private final ConcurrentMap<Cluster, View> perClusterViews;
    private volatile ScheduledExecutorService scheduler;
    private volatile boolean scheduled;

    public DefaultDiscovery(Router router, RemoteNodeFactory nodeFactory) {
        this.router = router;
        this.nodeFactory = nodeFactory;
        this.perClusterNodes = new ConcurrentHashMap<Cluster, List<Node>>();
        this.perClusterViews = new ConcurrentHashMap<Cluster, View>();
    }

    @Override
    public synchronized void join(Cluster cluster, String seed) throws MissingRouteException, ProcessingException {
        if (!cluster.isLocal()) {
            LOG.info("Joining cluster {} with seed {}", cluster, seed);
            String[] hostPortPair = seed.split(":");
            Node bootstrap = nodeFactory.makeNode(hostPortPair[0], Integer.parseInt(hostPortPair[1]), seed);
            try {
                bootstrap.connect();
                View view = requestMembership(cluster, Arrays.asList(bootstrap));
                calculateView(cluster, view);
            } catch (Exception ex) {
                LOG.warn(ex.getMessage(), ex);
                throw new MissingRouteException(new ErrorMessage(ErrorMessage.UNAVAILABLE_ERROR_CODE, "Seed is unavailable: " + seed));
            } finally {
                bootstrap.disconnect();
            }
        } else {
            throw new IllegalArgumentException("No need to join local cluster: " + cluster);
        }
    }

    @Override
    public synchronized void schedule(long delay, long interval, TimeUnit timeUnit) {
        if (!scheduled) {
            scheduler = Executors.newScheduledThreadPool(perClusterNodes.size());
            for (final Cluster cluster : perClusterNodes.keySet()) {
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
    public synchronized void cancel() {
        if (scheduled) {
            scheduler.shutdownNow();
            scheduled = false;
        }
    }

    private void update(Cluster cluster) throws MissingRouteException, ProcessingException {
        List<Node> nodes = perClusterNodes.get(cluster);
        View view = requestMembership(cluster, nodes);
        calculateView(cluster, view);
    }

    private View requestMembership(Cluster cluster, List<Node> contactNodes) throws MissingRouteException, ProcessingException {
        View view = null;
        Iterator<Node> nodeIterator = contactNodes.iterator();
        boolean successful = false;
        while (!successful && nodeIterator.hasNext()) {
            Node node = nodeIterator.next();
            view = node.<View>send(new MembershipCommand());
            successful = true;
        }
        if (view != null) {
            return view;
        } else {
            throw new MissingRouteException(new ErrorMessage(ErrorMessage.UNAVAILABLE_ERROR_CODE, "Missing route to cluster: " + cluster));
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
            Set<View.Member> leavingMembers = Sets.difference(currentView.getMembers(), updatedView.getMembers());
            Set<View.Member> joiningMembers = Sets.difference(updatedView.getMembers(), currentView.getMembers());
            for (View.Member member : leavingMembers) {
                Node node = findNode(currentNodes, member);
                router.removeRouteTo(cluster, node);
                node.disconnect();
                currentNodes.remove(node);
            }
            for (View.Member member : joiningMembers) {
                Node node = nodeFactory.makeNode(member.getHost(), member.getPort(), member.getName());
                router.addRouteTo(cluster, node);
                node.connect();
                currentNodes.add(node);
            }
        } else {
            perClusterViews.put(cluster, updatedView);
            for (View.Member member : updatedView.getMembers()) {
                Node node = nodeFactory.makeNode(member.getHost(), member.getPort(), member.getName());
                router.addRouteTo(cluster, node);
                node.connect();
                currentNodes.add(node);
            }
        }
    }

    private Node findNode(List<Node> nodes, Member member) {
        try {
            return Iterables.find(nodes, new NodeFinder(member.getName()));
        } catch (NoSuchElementException ex) {
            return null;
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
