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
package terrastore.partition;

import java.util.Set;
import terrastore.communication.Cluster;
import terrastore.store.Key;

/**
 * The EnsemblePartitioner manages ensemble clusters, creating a fixed partition table for configured clusters.
 *
 * @author Sergio Bossa
 */
public interface EnsemblePartitioner {

    /**
     * Set up {@link terrastore.communication.Cluster}s in a fixed partition table.
     *
     * @param clusters Clusters to set up.
     */
    public void setupClusters(Set<Cluster> clusters);

    /**
     * Get the {@link terrastore.communication.Cluster} corresponding to the given bucket name.
     *
     * @param bucket
     * @return The cluster corresponding to the given bucket.
     */
    public Cluster getClusterFor(String bucket);

    /**
     * Get the {@link terrastore.communication.Cluster} corresponding to the given bucket name and key.
     *
     * @param bucket
     * @param key
     * @return The cluster corresponding to the given bucket and key.
     */
    public Cluster getClusterFor(String bucket, Key key);
}
