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
package terrastore.service;

import java.util.Map;
import java.util.Set;
import terrastore.communication.CommunicationException;
import terrastore.decorator.failure.HandleFailure;
import terrastore.router.Router;
import terrastore.server.Buckets;
import terrastore.server.Keys;
import terrastore.server.ServerOperationException;
import terrastore.server.Values;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.store.features.Mapper;
import terrastore.store.features.Predicate;
import terrastore.store.features.Range;
import terrastore.store.features.Reducer;

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
     * @return A set of all bucket names.
     * @throws CommunicationException If unable to perform the operation due to cluster communication errors.
     * @throws QueryOperationException If unable to access buckets.
     */
    @HandleFailure(exception = CommunicationException.class)
    public Buckets getBuckets() throws CommunicationException, QueryOperationException;

    /**
     * Execute a bulk get from the given bucket.
     *
     * @param bucket The name of the bucket where to bulk get values.
     * @param keys The keys to get.
     * @return The values.
     * @throws ServerOperationException If an error occurs.
     */
    @HandleFailure(exception = CommunicationException.class)
    public Values bulkGet(String bucket, Keys keys) throws CommunicationException, QueryOperationException;

    /**
     * Get the value under the given key, contained by the given bucket.<br>
     * If a non-empty predicate is provided, the returned value must satisfy the given predicate as well.
     *
     * @param bucket The bucket containing the value.
     * @param key The key of the value to get.
     * @return The value.
     * @param predicate The predicate to evaluate; predicate can be null or empty.
     * @throws CommunicationException If unable to perform the operation due to cluster communication errors.
     * @throws QueryOperationException If a bucket with the given name, or value with the given key, do not exist.
     */
    @HandleFailure(exception = CommunicationException.class)
    public Value getValue(String bucket, Key key, Predicate predicate) throws CommunicationException, QueryOperationException;

    /**
     * Get all values contained by the given bucket.
     *
     * @param bucket The bucket whose key/values we want to get.
     * @param limit Max number of elements to retrieve; if zero, all values will be returned.
     * @return A map containing all key/value pairs
     * @throws CommunicationException If unable to perform the operation due to cluster communication errors.
     * @throws QueryOperationException If a bucket with the given name doesn't exist.
     */
    @HandleFailure(exception = CommunicationException.class)
    public Values getAllValues(String bucket, int limit) throws CommunicationException, QueryOperationException;

    /**
     * Execute a range query returning all key/value pairs whose key falls into the given range, and whose value satisfies the given predicate (if any).
     * <br><br>
     * Returned key/value pairs are ordered as determined by the comparator whose name matches the one contained in the
     * {@link terrastore.store.features.Range} object, and values satisfy the condition whose name matches the one contained in the
     * {@link terrastore.store.features.Predicate} object (if any).
     * <br><br>
     * The query is executed over a snapshot view of the bucket keys, so the timeToLive carried into the range object determines,
     * in milliseconds, the max snapshot age: if the snapshot is older than the given time, it's recomputed,
     * otherwise it will be actually used for the query.
     *
     * @param bucket The bucket to query.
     * @param range The range which keys must be fall into.
     * @param predicate The predicate to evaluate on values.
     * @return An ordered map containing key/value pairs.
     * @throws CommunicationException If unable to perform the operation due to cluster communication errors.
     * @throws QueryOperationException If a bucket with the given name doesn't exist, or no matching condition is found.
     */
    @HandleFailure(exception = CommunicationException.class)
    public Values queryByRange(String bucket, Range range, Predicate predicate) throws CommunicationException, QueryOperationException;

    /**
     * Execute a predicate-based query returning all key/value pairs whose value satisfies the given predicate.
     * <br><br>
     * Returned key/value pairs, in no particular order, satisfy the condition whose name matches the one contained in the
     * {@link terrastore.store.features.Predicate} object.
     *
     * @param bucket The bucket to query.
     * @param predicate The predicate to evaluate on values.
     * @return A map containing key/value pairs.
     * @throws CommunicationException If unable to perform the operation due to cluster communication errors.
     * @throws QueryOperationException If a bucket with the given name doesn't exist, or no condition is specified or no matching is found.
     */
    @HandleFailure(exception = CommunicationException.class)
    public Values queryByPredicate(String bucket, Predicate predicate) throws CommunicationException, QueryOperationException;

    /**
     * Execute a map-reduce query over the given bucket and within a given (optional) key {@link terrastore.store.features.Range}, with mapper, combiner and
     * reducer functions described into the {@link terrastore.store.features.Mapper} and {@link terrastore.store.features.Reducer} objects.
     * <br><br>
     * The returned value is a document as resulted from the map-reduce aggregation.
     *
     * @param bucket The bucket to query.
     * @param range The optional key range to query. If null (or empty), the query will be executed over the whole bucket.
     * @param mapper The mapper object describing the mapper/combiner functions.
     * @param reducer The reducer object describing the reducer function.
     * @throws CommunicationException If unable to perform the operation due to cluster communication errors.
     * @throws QueryOperationException If a bucket with the given name doesn't exist, or no mapper/combiner/reducer function is specified or no matching is found.
     */
    @HandleFailure(exception = CommunicationException.class)
    public Value queryByMapReduce(String bucket, Range range, Mapper mapper, Reducer reducer) throws CommunicationException, QueryOperationException;

    /**
     * Get the {@link terrastore.router.Router} instance used for routing actual query operations.
     *
     * @return The router instance.
     */
    public Router getRouter();
}
