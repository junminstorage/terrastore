package terrastore.ensemble.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.ensemble.EnsembleException;
import terrastore.ensemble.EnsembleManager;
import terrastore.ensemble.ProxyContactNode;
import terrastore.router.impl.HashFunction;

/**
 * @author Sergio Bossa
 */
public class HashingEnsembleManager implements EnsembleManager {

    private final ReadWriteLock stateLock;
    private final HashFunction hashFunction;
    private Cluster[] clusters;
    private Map<Cluster, ProxyContactNode> contactNodes;

    public HashingEnsembleManager(HashFunction hashFunction) {
        this.stateLock = new ReentrantReadWriteLock();
        this.hashFunction = hashFunction;
        this.clusters = new Cluster[0];
        this.contactNodes = new HashMap<Cluster, ProxyContactNode>();
    }

    @Override
    public void setupClusters(Set<Cluster> clusters) {
        stateLock.writeLock().lock();
        try {
            this.clusters = new Cluster[clusters.size()];
            int i = 0;
            for (Cluster cluster : clusters) {
                this.clusters[i++] = cluster;
                if (!cluster.isLocal()) {
                    this.contactNodes.put(cluster, new ProxyContactNode());
                }
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Cluster getClusterFor(String bucket) {
        stateLock.readLock().lock();
        try {
            int index = hashFunction.hash(bucket, clusters.length);
            return clusters[index];
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Cluster getClusterFor(String bucket, String key) {
        stateLock.readLock().lock();
        try {
            int index = hashFunction.hash(bucket + key, clusters.length);
            return clusters[index];
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void addContactNode(Cluster cluster, Node node) {
        stateLock.writeLock().lock();
        try {
            ProxyContactNode proxy = contactNodes.get(cluster);
            if (proxy != null) {
                proxy.addNode(node);
            } else {
                throw new EnsembleException("Cannot remove node " + node + " due to not configured remote cluster " + cluster);
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public void removeContactNode(Cluster cluster, Node node) {
        stateLock.writeLock().lock();
        try {
            ProxyContactNode proxy = contactNodes.get(cluster);
            if (proxy != null) {
                proxy.removeNode(node);
            } else {
                throw new EnsembleException("Cannot remove node " + node + " due to not configured remote cluster " + cluster);
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Node getContactNodeFor(Cluster cluster) {
        stateLock.readLock().lock();
        try {
            ProxyContactNode proxy = contactNodes.get(cluster);
            if (proxy != null) {
                return proxy;
            } else {
                throw new EnsembleException("Cannot get node for not configured remote cluster " + cluster);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }
}
