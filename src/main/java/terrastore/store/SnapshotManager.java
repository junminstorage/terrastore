/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.store;

import java.util.Comparator;

/**
 * Compute, store and retrieve {@link SortedSnapshot} instances of bucket keys.
 *
 * @author Sergio Bossa
 */
public interface SnapshotManager {

    /**
     * Get or compute a sorted snapshot of the given {@link Bucket} keys, using the given {@link java.util.Comparator}.<br/>
     * Every snapshot is identified and named by the <i>name</i> parameter.<br>
     * The choice between retrieving an alredy stored snapshot, or computing a new one, is taken based on the <i>timeToLive</i>: snapshots
     * with expired time to live will be recomputed.
     *
     * @param bucket The bucket for which taking a snapshot of the keys.
     * @param comparator The comparator to use for sorting keys.
     * @param name Name of the snapshot to retrieve or compute.
     * @param timeToLive Snapshot required time to live.
     * @return The {@link SortedSnapshot} instance.
     */
    public SortedSnapshot getOrComputeSortedSnapshot(Bucket bucket, Comparator<String> comparator, String name, long timeToLive);
}
