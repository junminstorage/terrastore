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
package terrastore.communication.protocol;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import terrastore.store.Bucket;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import terrastore.store.features.Range;

/**
 * @author Sergio Bossa
 */
public class DoRangeQueryCommand extends AbstractCommand {

    private final String bucketName;
    private final Range range;
    private final Comparator<String> keyComparator;

    public DoRangeQueryCommand(String bucketName, Range range, Comparator<String> keyComparator) {
        this.bucketName = bucketName;
        this.range = range;
        this.keyComparator = keyComparator;
    }

    public Map<String, Value> executeOn(Store store) throws StoreOperationException {
        Bucket bucket = store.get(bucketName);
        if (bucket != null) {
            Map<String, Value> entries = new HashMap<String, Value>();
            Set<String> keys = bucket.keysInRange(range, keyComparator);
            for (String key : keys) {
                entries.put(key, null);
            }
            return entries;
        } else {
            return new HashMap<String, Value>(0);
        }
    }
}
