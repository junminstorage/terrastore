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
package terrastore.store.aggregators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import terrastore.store.operators.Aggregator;
import static terrastore.util.concurrent.ConcurrentUtils.*;

/**
 * @author Sergio Bossa
 */
public class KeysAggregator implements Aggregator {

    @Override
    public Map<String, Object> apply(List<Map<String, Object>> keys, Map<String, Object> parameters) {
        Map<String, Object> result = new HashMap<String, Object>(1, 100);
        List<String> list = new ArrayList<String>(keys.size());
        for (Map<String, Object> key : keys) {
            exitOnTimeout();
            Object value = key.get("keys");
            if (value instanceof String) {
                list.add((String) value);
            } else {
                list.addAll((List<String>) value);
            }
        }
        result.put("keys", list);
        return result;
    }

}
