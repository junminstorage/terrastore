package terrastore.partition.impl;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import terrastore.communication.Cluster;
import terrastore.partition.EnsemblePartitioner;
import terrastore.router.impl.HashFunction;

/**
 * @author Sergio Bossa
 */
public class EnsembleHashingPartitioner implements EnsemblePartitioner {

    private final ReadWriteLock stateLock;
    private final HashFunction hashFunction;
    private Cluster[] clusters;

    public EnsembleHashingPartitioner(HashFunction hashFunction) {
        this.stateLock = new ReentrantReadWriteLock();
        this.hashFunction = hashFunction;
        this.clusters = new Cluster[0];
    }

    @Override
    public void setupClusters(Set<Cluster> clusters) {
        stateLock.writeLock().lock();
        try {
            this.clusters = new Cluster[clusters.size()];
            int i = 0;
            for (Cluster cluster : clusters) {
                this.clusters[i++] = cluster;
            }
            Arrays.sort(this.clusters);
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
}
