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
package terrastore.service;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import terrastore.router.Router;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.features.Range;
import terrastore.store.operators.Condition;

/**
 * The QueryService manages the operations for finding values in buckets, by interacting with a {@link terrastore.router.Router}
 * instance, in order to route the specific query operation to a particular {@link terrastore.communication.Node}, which can be
 * the same local node, or rather a remote node (depending on the router implementation).
 *
 * @author Sergio Bossa
 */
public interface QueryService {

    /**
     * Get the name of all buckets.
     *
     * @return A collection of all bucket names.
     * @throws QueryOperationException If unable to access buckets.
     */
    public Collection<String> getBuckets() throws QueryOperationException;

    /**
     * Get the value under the given key, contained by the given bucket.
     *
     * @param bucket The bucket containing the value.
     * @param key The key of the value to get.
     * @return The value.
     * @throws QueryOperationException If a bucket with the given name, or value with the given key, do not exist.
     */
    public Value getValue(String bucket, String key) throws QueryOperationException;

    /**
     * Get all values contained by the given bucket.
     *
     * @param bucket The bucket whose key/values we want to get.
     * @return A map containing all key/value pairs
     * @throws QueryOperationException If a bucket with the given name doesn't exist.
     */
    public Map<String, Value> getAllValues(String bucket) throws QueryOperationException;

    /**
     * Execute a range query returning all key/value pairs whose key falls into the given range, and whose value satisfies the given predicate (if any).
     * <br><br>
     * Returned key/value pairs are ordered as determined by the comparator whose name matches the one contained in the
     * {@link terrastore.store.features.Range} object, and values satisfy the condition whose name matches the one contained in the
     * {@link terrastore.store.features.Predicate} object (if any).
     * <br><br>
     * Comparators are provided by the {@link #getComparators()} method; if no matching comparator is found,
     * the default one is used (see {@link #getDefaultComparator()}).<br>
     * Conditions are provided by the {@link #getConditions()} method; if the predicate doesn't specify any condition,
     * no condition will be used hence all values in range will be returned; if the specified condition is not found, an exception is thrown.
     * <br><br>
     * The query is executed over a snapshot view of the bucket keys, so the timeToLive parameter determines,
     * in milliseconds, the max snapshot age: if the snapshot is older than the given time, it's recomputed,
     * otherwise it will be actually used for the query.
     *
     * @param bucket The bucket to query.
     * @param range The range which keys must be fall into.
     * @param predicate The predicate to evaluate on values.
     * @param timeToLive Number of milliseconds specifying the snapshot age; if set to 0, a new snapshot will be immediately computed
     * and the query executed on the fresh snasphot.
     * @return An ordered map containing key/value pairs.
     * @throws QueryOperationException If a bucket with the given name doesn't exist, or no matching condition is found.
     */
    public Map<String, Value> doRangeQuery(String bucket, Range range, Predicate predicate, long timeToLive) throws QueryOperationException;

    /**
     * Execute a predicate-based query returning all key/value pairs whose value satisfies the given predicate.
     * <br><br>
     * Returned key/value pairs, in no particular order, satisfy the condition whose name matches the one contained in the
     * {@link terrastore.store.features.Predicate} object.
     * <br><br>
     * Conditions are provided by the {@link #getConditions()} method; if the predicate doesn't specify any condition,
     * or the condition is not found, an exception is thrown.
     *
     * @param bucket The bucket to query.
     * @param predicate The predicate to evaluate on values.
     * @return A map containing key/value pairs.
     * @throws QueryOperationException If a bucket with the given name doesn't exist, or no condition is specified or no matching is found.
     */
    public Map<String, Value> doPredicateQuery(String bucket, Predicate predicate) throws QueryOperationException;

    /**
     * Get the {@link terrastore.router.Router} instance used for routing actual query operations.
     *
     * @return The router instance.
     */
    public Router getRouter();

    /**
     * Get the default {@link java.util.Comparator} used to compare keys when no other comparator is found.
     *
     * @return The default comparator.
     */
    public Comparator<String> getDefaultComparator();

    /**
     * Get all supported {@link java.util.Comparator} by name.
     *
     * @return A map of supported comparators.
     */
    public Map<String, Comparator<String>> getComparators();

    /**
     * Get all supported {@link terrastore.store.operators.Condition} by name.
     *
     * @return A map of supported conditions.
     */
    public Map<String, Condition> getConditions();
}
