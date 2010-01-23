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
package terrastore.store.impl;

import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.store.FlushCondition;
import terrastore.store.FlushStrategy;
import terrastore.store.Bucket;
import terrastore.store.FlushCallback;

/**
 * Sequentially doFlush all key/value entries in a given bucket.<br>
 * This doFlush strategy has a O(n) complexity, where n is the total number of
 * entries in the bucket.
 *
 * @author Sergio Bossa
 */
public class SequentialFlushStrategy implements FlushStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(SequentialFlushStrategy.class);

    @Override
    public void flush(Bucket bucket, Collection<String> keys, FlushCondition flushCondition, FlushCallback flushCallback) {
        for (String key : keys) {
            if (flushCondition.isSatisfied(bucket, key)) {
                flushCallback.doFlush(key);
                LOG.debug("Flushed key: {}", key);
            }
        }
    }
}
