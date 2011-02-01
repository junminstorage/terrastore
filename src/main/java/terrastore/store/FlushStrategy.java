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
package terrastore.store;

import java.util.Collection;

/**
 * Define a flush strategy to apply for flushing key/value entries of all buckets contained into
 * the store.
 *
 * @author Sergio Bossa
 */
public interface FlushStrategy {

    /**
     * Flush all given keys.
     * <br>
     * The actual decision whether the key must be flushed or not, is left to the given {@link FlushCondition}.
     *
     * @param bucket The bucket containing the keys to flush.
     * @param keys The keys to evaluate for flushing.
     * @param flushCondition The condition to evaluate for key flushing.
     * @param flushCallback Execute the actual flushing.
     */
    public void flush(Bucket bucket, Collection<Key> keys, FlushCondition flushCondition, FlushCallback flushCallback);
}
