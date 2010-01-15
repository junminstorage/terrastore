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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import terrastore.store.Bucket;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;

/**
 * @author Sergio Bossa
 */
public class GetBucketsCommand extends AbstractCommand {

    public Map<String, Value> executeOn(Store store) throws StoreOperationException {
        Collection<Bucket> buckets = store.buckets();
        Map<String, Value> names = new HashMap<String, Value>();
        for (Bucket bucket : buckets) {
            names.put(bucket.getName(), null);
        }
        return names;
    }
}
