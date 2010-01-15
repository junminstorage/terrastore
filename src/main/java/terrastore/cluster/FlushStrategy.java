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
package terrastore.cluster;

import terrastore.store.Store;

/**
 * Define a flush strategy to apply for flushing key/value entries of all buckets contained into
 * the store.
 *
 * @author Sergio Bossa
 */
public interface FlushStrategy {

    /**
     * Flush all key/value entries of all buckets contained into the given store.
     * <br>
     * The actual decision whether the key must be flushed or not, is left to the given {@link FlushCondition}.
     *
     * @param store The store whose entries should be flushed.
     * @param flushCondition The condition to evaluate for key flushing.
     */
    public void flush(Store store, FlushCondition flushCondition);
}
