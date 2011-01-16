/**
 * Copyright 2009 - 2010 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.server;

import terrastore.common.ClusterStats;
import terrastore.service.BackupService;
import terrastore.service.QueryService;
import terrastore.service.UpdateService;
import terrastore.store.Key;
import terrastore.store.Value;

/**
 * The Server handles client requests relying on services to actually execute them.
 *
 * @author Sergio Bossa
 */
public interface Server {

    /**
     * Remove the given bucket.
     *
     * @param bucket The name of the bucket to remove.
     * @throws ServerOperationException If an error occurs.
     */
    public void removeBucket(String bucket) throws ServerOperationException;

    /**
     * Put a value in the given bucket under the given key.<br>
     * Conditional put can be executed by providing a predicate expression:
     * in such a case, the new value will be put only if no value existed before, or the existent value satisfies the given predicate.
     *
     * @param bucket The name of the bucket where to put the value.
     * @param key The key of the value to put.
     * @param value The value to put.
     * @param predicate The predicate to evaluate in case of conditional put, or null for no predicate.
     * @throws ServerOperationException If an error occurs.
     */
    public void putValue(String bucket, Key key, Value value, String predicate) throws ServerOperationException;

    /**
     * Remove a value from the given bucket under the given key.
     *
     * @param bucket The name of the bucket where to remove the value from.
     * @param key The key of the value to remove.
     * @throws ServerOperationException If an error occurs.
     */
    public void removeValue(String bucket, Key key) throws ServerOperationException;

    /**
     * Execute an update on a value from the given bucket under the given key.
     * 
     * @param bucket The name of the bucket holding the value to update.
     * @param key The key of the value to update.
     * @param function The name of the server-side function performing the actual update.
     * @param timeoutInMillis The timeout for the update operation (update operations lasting more than the given timeout will be aborted).
     * @param parameters The update operation parameters.
     * @return The updated value
     * @throws ServerOperationException If an error occurs.
     */
    public Value updateValue(String bucket, Key key, String function, Long timeoutInMillis, Parameters parameters) throws ServerOperationException;

    /**
     * Execute an update on a value from the given bucket under the given key.
     *
     * @param bucket The name of the bucket holding the value to update.
     * @param key The key of the value to update.
     * @param function The name of the server-side function performing the actual update.
     * @param timeoutInMillis The timeout for the update operation (update operations lasting more than the given timeout will be aborted).
     * @param parameters The update operation parameters.
     * @return The updated value
     * @throws ServerOperationException If an error occurs.
     */
    public Value mergeValue(String bucket, Key key, Value value) throws ServerOperationException;

    /**
     * Get the name of all buckets.
     *
     * @return A collection of all bucket names.
     * @throws ServerOperationException If an error occurs.
     */
    public Buckets getBuckets() throws ServerOperationException;

    /**
     * Get the value from the given bucket under the given key.<br>
     * If a non-empty predicate is provided, the returned value must satisfy the given predicate as well.
     *
     * @param bucket The name of the bucket containing the value to get.
     * @param key The key of the value to get.
     * @return The value.
     * @param predicate The predicate to evaluate; predicate can be null or empty.
     * @throws ServerOperationException If an error occurs.
     */
    public Value getValue(String bucket, Key key, String predicate) throws ServerOperationException;

    /**
     * Get all key/value entries into the given bucket.
     *
     * @param bucket The name of the bucket containing the values to get.
     * @param limit Max number of elements to retrieve; if zero, all values will be returned.
     * @return A map containing all key/value entries.
     * @throws ServerOperationException If an error occurs.
     */
    public Values getAllValues(String bucket, int limit) throws ServerOperationException;

    /**
     * Execute a range query returning all key/value pairs whose key falls into the given range, and whose value satisfies the given predicate (if any).
     * <br><br>
     * The selected range goes from start key to end key, with the max number of elements equal to the provided limit.<br>
     * If the limit is zero, all the elements in the range will be selected.<br>
     * If no end key is provided, all elements starting from start key and up to the limit will be selected.
     * <br><br>
     * The range query is executed over a snapshot view of the bucket keys, so the timeToLive parameter determines,
     * in milliseconds, the snapshot max age: if the snapshot is older than the given time, it's recomputed,
     * otherwise it will be actually used for the query.
     *
     * @param bucket The bucket to query.
     * @param startKey First key in range.
     * @param endKey Last key in range (inclusive); if null, all elements starting from start key and up to the limit will be selected.
     * @param limit Max number of elements to retrieve (even if not reaching the end of the range); if zero, all elements in range will be selected.
     * @param comparator Name of the comparator to use for testing if a key is in range.
     * @param predicate The predicate to evaluate (if any).
     * @param timeToLive Number of milliseconds specifying the snapshot age; if set to 0, a new snapshot will be immediately computed
     * and the query executed on the fresh snasphot.
     * @return A map containing key/value pairs
     * @throws ServerOperationException If an error occurs.
     */
    public Values queryByRange(String bucket, Key startKey, Key endKey, int limit, String comparator, String predicate, long timeToLive) throws ServerOperationException;

    /**
     * Remove all key/value pairs whose key falls wihtin the given range, and whose value satisfies the given predicate (if any).
     * <br><br>
     * the selected range goes from start key to end key, with the max number of elements equal to the provided limit.<br>
     * The limit specifies the max number of key/value pairs to examine and possibly delete. If the limit is lower than the number of
     * keys in the range and a predicate is used, the number of actually removed key/value pairs may be lower than the limit, while 
     * values matching the predicate may still remain in the bucket.
     * If the limit is set to zero, all key/value pairs in the range will be examined.
     * <br><br> 
     * If no end key is provided, all elements starting from the start key and up to the limit will be removed.
     * <br><br>
     * The range query is executed over a snapshot view of the bucket keys, so the timeToLive parameter determines,
     * in milliseconds, the snapshot max age: if the snapshot is older than the given time, it's recomputed,
     * otherwise it will be actually used for the query.
     * 
     * @param bucket The bucket to query.
     * @param startKey First key in range.
     * @param endKey Last key in range (inclusive); if null, all elements starting from start key and up to the limit will be removed.
     * @param limit Max number of keys to examine/elements to remove (even if not reaching the end of the range); if zero, all elements in range will be deleted.
     * @param comparator Name of the comparator to use for testing if a key is in range.
     * @param predicate The predicate to evaluate against values (optional).
     * @param timeToLive Number of milliseconds specifying the snapshot age; if set to 0, a new snapshot will be immediately computed
     * for the delete operation to be executed on.
     * @return An unordered {@link Keys} set containing the keys that were actually removed.
     */
    public Keys removeByRange(String bucket, Key startKey, Key endKey, int limit, String comparator, String predicate, long timeToLive) throws ServerOperationException;

    /**
     * Execute a predicate-based query returning all key/value pairs whose value satisfies the given predicate.
     *
     * @param bucket The bucket to query.
     * @param predicate The predicate to evaluate.
     * @return A map containing key/value pairs
     * @throws ServerOperationException If an error occurs.
     */
    public Values queryByPredicate(String bucket, String predicate) throws ServerOperationException;

    /**
     * Execute a map-reduce query over the given bucket.
     *
     * @param bucket The bucket to query.
     * @param descriptor The map-reduce query descriptor.
     * @throws ServerOperationException If an error occurs.
     */
    public Value queryByMapReduce(String bucket, MapReduceDescriptor descriptor) throws ServerOperationException;

    /**
     * Execute the import of all bucket key/value entries, without interrupting other operations and preserving
     * existent entries not contained into the given backup.
     *
     * @param bucket The bucket to import entries to.
     * @param source The name of the resource from which reading the backup.
     * @param secret The secret key: import is executed only if it matches the pre-configured secret.
     * @throws ServerOperationException If an error occurs.
     */
    public void importBackup(String bucket, String source, String secret) throws ServerOperationException;

    /**
     * Execute the export of all bucket key/value entries, without interrupting other operations.
     *
     * @param bucket The bucket to export entries from.
     * @param destination The name of the resource into which writing the backup.
     * @param secret The secret key: export is executed only if it matches the pre-configured secret.
     * @throws ServerOperationException If an error occurs.
     */
    public void exportBackup(String bucket, String destination, String secret) throws ServerOperationException;

    /**
     * Get the current {@link terrastore.common.ClusterStats}.
     *
     * @return The {@link terrastore.common.ClusterStats} instance.
     */
    public ClusterStats getClusterStats();

    /**
     * Get the {@link terrastore.service.UpdateService} which will actually execute all update operations.
     *
     * @return The {@link terrastore.service.UpdateService} instance.
     */
    public UpdateService getUpdateService();

    /**
     * Get the {@link terrastore.service.QueryService} which will actually execute all query operations.
     *
     * @return The {@link terrastore.service.QueryService} instance.
     */
    public QueryService getQueryService();

    /**
     * Get the {@link terrastore.service.BackupService} which will actually execute all backup operations.
     *
     * @return The {@link terrastore.service.BackupService} instance.
     */
    public BackupService getBackupService();

}
