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
package terrastore.communication.protocol;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import terrastore.store.Bucket;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.features.Range;

/**
 * @author Sergio Bossa
 */
public class RangeQueryCommand extends AbstractCommand<Set<String>> {

    private final String bucketName;
    private final Range range;
    private final Comparator<String> keyComparator;
    private final long timeToLive;

    public RangeQueryCommand(String bucketName, Range range, Comparator<String> keyComparator, long timeToLive) {
        this.bucketName = bucketName;
        this.range = range;
        this.keyComparator = keyComparator;
        this.timeToLive = timeToLive;
    }

    public Set<String> executeOn(Store store) throws StoreOperationException {
        Bucket bucket = store.get(bucketName);
        if (bucket != null) {
            Set<String> keys = bucket.keysInRange(range, keyComparator, timeToLive);
            return keys;
        } else {
            return Collections.<String>emptySet();
        }
    }
}
