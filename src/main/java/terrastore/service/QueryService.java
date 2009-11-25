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
package terrastore.service;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import terrastore.router.Router;
import terrastore.store.Value;
import terrastore.store.features.Range;

/**
 * The QueryService manages the operations for finding values in buckets, by interacting with a {@link terrastore.router.Router}
 * instance, in order to route the specific query operation to a particular {@link terrastore.communication.Node}, which can be
 * the same local node, or rather a remote node (depending on the router implementation).
 *
 * @author Sergio Bossa
 */
public interface QueryService {

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
     * Execute a range query returning all key/value pairs whose key falls into the given range, as determined by the comparator whose
     * name matches the comparator name reported in the range parameter.<br>
     * Returned key/value pairs are ordered as determined by the comparator above.<br>
     * Comparators are provided by the {@link #getComparators()} method; if no matching comparator is found,
     * the default one is used (see {@link #getDefaultComparator()}).
     *
     * @param bucket The bucket to query.
     * @param keyRange The range which keys must be fall into.
     * @return An ordered map containing key/value pairs.
     * @throws QueryOperationException If a bucket with the given name doesn't exist.
     */
    public Map<String, Value> doRangeQuery(String bucket, Range keyRange) throws QueryOperationException;

    /**
     * Get the {@link terrastore.router.Router} instance used for routing actual query operations.
     *
     * @return The router instance.
     */
    public Router getRouter();

    /**
     * Get the default comparator used to compare keys when no other comparator is found.
     *
     * @return The default comparator.
     */
    public Comparator<String> getDefaultComparator();

    /**
     * Get all supported comparators by name, used to compare keys.
     *
     * @return A map of supported comparators.
     */
    public Map<String, Comparator<String>> getComparators();
}
