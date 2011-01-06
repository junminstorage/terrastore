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

import java.util.List;

import terrastore.communication.CommunicationException;
import terrastore.decorator.failure.HandleFailure;
import terrastore.router.Router;
import terrastore.server.Keys;
import terrastore.store.Key;
import terrastore.store.features.Range;
import terrastore.store.features.Update;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.ValidationException;

/**
 * The UpdateService manages the operations of add and removal of buckets and values by interacting with a {@link terrastore.router.Router}
 * instance, in order to route the specific update operation to a particular {@link terrastore.communication.Node}, which can be
 * the same local node, or rather a remote node (depending on the router implementation).
 *
 * @author Sergio Bossa
 */
public interface UpdateService {

    /**
     * Remove the given bucket.
     *
     * @param bucket The name of the bucket to remove.
     * @throws CommunicationException If unable to perform the operation due to cluster communication errors.
     * @throws UpdateOperationException If no bucket is found with the given name.
     */
    @HandleFailure(exception = CommunicationException.class)
    public void removeBucket(String bucket) throws CommunicationException, UpdateOperationException;

    /**
     * Put a value into the given bucket under the given key, eventually replacing the old value.<br>
     * If a {@link terrastore.store.features.Predicate} is provided, and the predicate isn't empty see {@link terrastore.store.features.Predicate#isEmpty()},
     * the new value will be actually put only if no value existed before, or the old value satisfies the given predicate.
     *
     * @param bucket The name of the bucket to put the value into.
     * @param key The key of the value.
     * @param value The value to put.
     * @param predicate The predicate object containing the condition to evaluate.
     * @throws CommunicationException If unable to perform the operation due to cluster communication errors.
     * @throws UpdateOperationException If a bucket with the given name, or value with the given key, do not exist.
     */
    @HandleFailure(exception = CommunicationException.class)
    public void putValue(String bucket, Key key, Value value, Predicate predicate) throws CommunicationException, UpdateOperationException, ValidationException;

    /**
     * Remove a value from the given bucket under the given key.
     *
     * @param bucket The name of the bucket to remove the value from.
     * @param key The key of the value.
     * @throws CommunicationException If unable to perform the operation due to cluster communication errors.
     * @throws UpdateOperationException If a bucket with the given name, or value with the given key, do not exist.
     */
    @HandleFailure(exception = CommunicationException.class)
    public void removeValue(String bucket, Key key) throws CommunicationException, UpdateOperationException;

    /**
     * TODO: Document
     * @param bucket
     * @param range
     * @param predicate
     * @return
     */
	public Keys removeByRange(String bucket, Range range, Predicate predicate) throws CommunicationException, UpdateOperationException;
    
    /**
     * Update the value from the given bucket under the given key.
     *
     * @param bucket The name of the bucket holding the value to update.
     * @param key The key of the value to update.
     * @param update The update object.
     * @return The updated value
     * @throws CommunicationException If unable to perform the operation due to cluster communication errors.
     * @throws UpdateOperationException If errors occur during update.
     */
    @HandleFailure(exception = CommunicationException.class)
    public Value updateValue(String bucket, Key key, Update update) throws CommunicationException, UpdateOperationException;

    /**
     * Get the {@link terrastore.router.Router} instance used for routing actual update operations.
     *
     * @return The router instance.
     */
    public Router getRouter();

}
