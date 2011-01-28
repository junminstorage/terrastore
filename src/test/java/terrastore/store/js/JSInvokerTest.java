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
package terrastore.store.js;

import org.junit.Test;
import terrastore.store.Value;
import terrastore.util.json.JsonUtils;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import terrastore.util.collect.Maps;
import static org.junit.Assert.*;

/**
 * @author Giuseppe Santoro
 * @author Sergio Bossa
 */
public class JSInvokerTest {

    private static final String JSON_VALUE = "{\"test\":\"test\"}";

    @Test
    public void testWithFunction() throws Exception {
        Map<String, Object> params = new HashMap<String, Object>();
        String f = "function(key, value, params) {"
                + "   if(value['test'] == 'test')"
                + "       value['test'] = params.newValue;"
                + "   return value;"
                + "}";
        params.put("function", f);
        params.put("newValue", "test2");

        JSInvoker function = new JSInvoker("function");
        Value value = new Value(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> newMap = function.apply("key", JsonUtils.toModifiableMap(value), params);

        assertEquals("test2", newMap.get("test"));
    }

    @Test
    public void testWithAggregator() throws Exception {
        List<Map<String, Object>> values = new LinkedList<Map<String, Object>>();
        Map<String, Object> params = new HashMap<String, Object>();
        String f = "function(values, params) {"
                + "   var sum = 0;"
                + "   for (index in values) {"
                + "       sum = sum + values[index]['value']"
                + "   }"
                + "   return {'result' : sum};"
                + "}";
        values.add(Maps.hash(new String[]{"value"}, new Object[]{1}));
        values.add(Maps.hash(new String[]{"value"}, new Object[]{2}));
        params.put("aggregator", f);

        JSInvoker function = new JSInvoker("aggregator");
        Map<String, Object> result = function.apply(values, params);

        assertEquals(3, result.get("result"));
    }
}
