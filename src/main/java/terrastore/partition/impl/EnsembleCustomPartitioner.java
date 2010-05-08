package terrastore.partition.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import terrastore.communication.Cluster;
import terrastore.partition.CustomEnsemblePartitionerStrategy;
import terrastore.partition.EnsemblePartitioner;
import terrastore.util.collect.Maps;
import terrastore.util.collect.support.ReflectionKeyExtractor;

/**
 * {@link terrastore.partition.EnsemblePartitioner} implementation delegating to
 * {@link EnsemblePartitionerStrategy} for actual partitioning.
 *
 * @author Sergio Bossa
 */
public class EnsembleCustomPartitioner implements EnsemblePartitioner {

    private final ReadWriteLock stateLock;
    private final CustomEnsemblePartitionerStrategy strategy;
    private final Map<String, Cluster> clusters;

    public EnsembleCustomPartitioner(CustomEnsemblePartitionerStrategy strategy) {
        this.strategy = strategy;
        this.stateLock = new ReentrantReadWriteLock();
        this.clusters = new HashMap<String, Cluster>();
    }

    @Override
    public void setupClusters(Set<Cluster> clusters) {
        stateLock.writeLock().lock();
        try {
            Maps.<String, Cluster>fill(this.clusters, new ReflectionKeyExtractor<String, Cluster>("name"), clusters.toArray(new Cluster[clusters.size()]));
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Cluster getClusterFor(String bucket) {
        stateLock.readLock().lock();
        try {
            CustomEnsemblePartitionerStrategy.Cluster cluster = strategy.getClusterFor(bucket);
            if (cluster != null) {
                return clusters.get(cluster.getName());
            } else {
                throw new IllegalStateException("Null partition for bucket " + bucket);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public Cluster getClusterFor(String bucket, String key) {
        stateLock.readLock().lock();
        try {
            CustomEnsemblePartitionerStrategy.Cluster cluster = strategy.getClusterFor(bucket, key);
            if (cluster != null) {
                return clusters.get(cluster.getName());
            } else {
                throw new IllegalStateException("Null partition for bucket " + bucket + " and key " + key);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }
}
