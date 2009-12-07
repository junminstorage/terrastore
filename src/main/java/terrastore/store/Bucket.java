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

import terrastore.store.features.Update;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import terrastore.store.function.Function;
import terrastore.store.features.Range;

/**
 * Bucket interface collecting and managing key/value entries.
 *
 * @author Sergio Bossa
 */
public interface Bucket {

    /**
     * Get the bucket name.
     *
     * @return The bucket name.
     */
    public String getName();

    /**
     * Put the given {@link Value} into this bucket under the given key,
     * eventually replacing the old one.
     *
     * @param key The key of the value to put.
     * @param value The value to put.
     */
    public void put(String key, Value value);

    /**
     * Get the {@link Value} under the given key.
     *
     * @param key The key of the value to get.
     * @return The value corresponding to the given key.
     * @throws StoreOperationException If no value is found for the given key.
     */
    public Value get(String key) throws StoreOperationException;

    /**
     * Remove the {@link Value} under the given key.
     *
     * @param key The key of the value to remove.
     * @throws StoreOperationException If no value to remove is found for the given key.
     */
    public void remove(String key) throws StoreOperationException;

    /**
     * Update the {@link Value} under the given key.
     *
     * @param key The key of the value to update.
     * @param update The update object.
     * @param function The function to apply for the update.
     * @param updateExecutor The executor to use for performing the update operation.
     * @throws StoreOperationException If errors occur during updating.
     */
    public void update(String key, Update update, Function function, ExecutorService updateExecutor) throws StoreOperationException;

    /**
     * Get all keys contained into this bucket.
     *
     * @return The set of keys.
     */
    public Set<String> keys();

    /**
     * Get a sorted set of all keys falling into the given range, as determined by the given comparator.
     *
     * @param keyRange The range which keys must be fall into.
     * @param keyComparator The comparator determining if a key falls into range.
     * @return The sorted set of keys in range.
     */
    public SortedSet<String> keysInRange(Range keyRange, Comparator<String> keyComparator);
}
