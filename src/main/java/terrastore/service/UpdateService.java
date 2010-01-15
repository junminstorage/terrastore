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

import java.util.Map;
import terrastore.router.Router;
import terrastore.store.features.Update;
import terrastore.store.Value;
import terrastore.store.operators.Function;

/**
 * The UpdateService manages the operations of add and removal of buckets and values by interacting with a {@link terrastore.router.Router}
 * instance, in order to route the specific update operation to a particular {@link terrastore.communication.Node}, which can be
 * the same local node, or rather a remote node (depending on the router implementation).
 *
 * @author Sergio Bossa
 */
public interface UpdateService {

    /**
     * Add the given bucket.
     *
     * @param bucket The name of the bucket to add.
     * @throws UpdateOperationException If a bucket with same name already exists.
     */
    public void addBucket(String bucket) throws UpdateOperationException;

    /**
     * Remove the given bucket.
     *
     * @param bucket The name of the bucket to remove.
     * @throws UpdateOperationException If no bucket is found with the given name.
     */
    public void removeBucket(String bucket) throws UpdateOperationException;

    /**
     * Put a value into the given bucket under the given key.
     *
     * @param bucket The name of the bucket to put the value into.
     * @param key The key of the value.
     * @param value The value to put.
     * @throws UpdateOperationException If a bucket with the given name, or value with the given key, do not exist.
     */
    public void putValue(String bucket, String key, Value value) throws UpdateOperationException;

    /**
     * Remove a value from the given bucket under the given key.
     *
     * @param bucket The name of the bucket to remove the value from.
     * @param key The key of the value.
     * @throws UpdateOperationException If a bucket with the given name, or value with the given key, do not exist.
     */
    public void removeValue(String bucket, String key) throws UpdateOperationException;

    /**
     * Execute an update on a value from the given bucket under the given key.
     *
     * @param bucket The name of the bucket holding the value to update.
     * @param key The key of the value to update.
     * @param update The update object.
     * @throws UpdateOperationException If errors occur during update.
     */
    public void executeUpdate(String bucket, String key, Update update) throws UpdateOperationException;

    /**
     *
     * @return
     */
    public Map<String, Function> getFunctions();

    /**
     * Get the {@link terrastore.router.Router} instance used for routing actual update operations.
     *
     * @return The router instance.
     */
    public Router getRouter();
}
