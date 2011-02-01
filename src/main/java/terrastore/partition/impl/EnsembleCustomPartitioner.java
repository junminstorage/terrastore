/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.partition.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import terrastore.communication.Cluster;
import terrastore.partition.CustomEnsemblePartitionerStrategy;
import terrastore.partition.EnsemblePartitioner;
import terrastore.store.Key;
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
    public Cluster getClusterFor(String bucket, Key key) {
        stateLock.readLock().lock();
        try {
            CustomEnsemblePartitionerStrategy.Cluster cluster = strategy.getClusterFor(bucket, key.toString());
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
