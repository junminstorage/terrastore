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

import terrastore.store.Bucket;

/**
 * Predicate-style interface to implement for defining if a key (and related value),
 * belonging to a given bucket, should be flushed.
 *
 * @author Sergio Bossa
 */
public interface FlushCondition {

    /**
     * Define if the key shoud be flushed.
     *
     * @param bucket The bucket containing the key/value to flush.
     * @param key The key to eventually flush.
     * @return True if to be flushed, false otherwise.
     */
    public boolean isSatisfied(Bucket bucket, String key);
}
