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

import java.util.Collection;

/**
 * Store interface for managing {@link Bucket} instances.
 *
 * @author Sergio Bossa
 */
public interface Store {

    /**
     * Add a named {@link Bucket}.<br>
     * Bucket names must be unique.
     *
     * @param bucket The bucket name.
     * @throws StoreOperationException If the bucket already exists.
     */
    public void add(String bucket) throws StoreOperationException;

    /**
     * Removed a named {@link Bucket}.
     *
     * @param bucket The bucket name.
     * @throws StoreOperationException If the bucket doesn't exist.
     */
    public void remove(String bucket) throws StoreOperationException;

    /**
     * Get a named {@link Bucket}.
     *
     * @param bucket The bucket name.
     * @return The named bucket.
     * @throws StoreOperationException If the bucket doesn't exist.
     */
    public Bucket get(String bucket) throws StoreOperationException;

    /**
     *
     * 
     * @return
     */
    public Collection<Bucket> buckets();
}
