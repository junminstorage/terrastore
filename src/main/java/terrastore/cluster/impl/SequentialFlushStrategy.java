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
package terrastore.cluster.impl;

import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.cluster.FlushCondition;
import terrastore.cluster.FlushStrategy;
import terrastore.store.Bucket;
import terrastore.store.Store;

/**
 * Sequentially flush all key/value entries of all buckets in the store.<br>
 * This flush strategy has a O(n) complexity, where n is the total number of
 * entries in all store buckets.
 *
 * @author Sergio Bossa
 */
public class SequentialFlushStrategy implements FlushStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(SequentialFlushStrategy.class);

    @Override
    public void flush(Store store, FlushCondition flushCondition) {
        for (Bucket bucket : store.buckets()) {
            LOG.info("Flushing bucket {}", bucket.getName());
            Set<String> keys = bucket.keys();
            Set<String> keysToFlush = new HashSet<String>();
            for (String key : keys) {
                if (flushCondition.isSatisfied(bucket, key)) {
                    LOG.debug("Flushing key {} on bucket {}", key, bucket);
                    keysToFlush.add(key);
                }
            }
            LOG.info("Flushing keys: {}", keysToFlush.size());
            bucket.flush(keysToFlush);
        }
    }
}
