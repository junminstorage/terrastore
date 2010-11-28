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
package terrastore.store;

/**
 * Lock manager interface for locking read/write operations.
 *
 * @author Sergio Bossa
 */
public interface LockManager {

    /**
     * Acquire a read lock over the specified bucket and key.
     *
     * @param bucket The bucket to lock.
     * @param key The key to lock.
     */
    public void lockRead(String bucket, Key key);

    /**
     * Release a read lock for the specified bucket and key.
     *
     * @param bucket The bucket to unlock.
     * @param key The key to unlock.
     */
    public void unlockRead(String bucket, Key key);

    /**
     * Acquire a write lock over the specified bucket and key.
     *
     * @param bucket The bucket to lock.
     * @param key The key to lock.
     */
    public void lockWrite(String bucket, Key key);

    /**
     * Release a write lock for the specified bucket and key.
     *
     * @param bucket The bucket to unlock.
     * @param key The key to unlock.
     */
    public void unlockWrite(String bucket, Key key);

    /**
     * Evict unused locks for the node identified by the given name.
     *
     * @param node The name of the node whose locks must be evicted.
     */
    public void evictLocks(String node);

}
