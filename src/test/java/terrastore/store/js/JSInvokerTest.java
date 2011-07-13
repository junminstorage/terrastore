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
package terrastore.store.js;

import org.junit.Test;
import terrastore.store.Value;
import terrastore.util.json.JsonUtils;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import terrastore.startup.Constants;
import terrastore.util.collect.Maps;
import static org.junit.Assert.*;

/**
 * @author Giuseppe Santoro
 * @author Sergio Bossa
 */
public class JSInvokerTest {

    private static final String JSON_VALUE = "{\"test\":\"test\"}";

    @Before
    public void before() {
        System.setProperty(Constants.TERRASTORE_HOME, this.getClass().getClassLoader().getResource("home").getFile());
    }

    @Test
    public void testWithFunction() throws Exception {
        Map<String, Object> params = new HashMap<String, Object>();
        String f = "function(key, value, params) {"
                + "    result = {};"
                + "    result[key] = params.newValue;"
                + "    return result;"
                + "}";
        params.put("function", f);
        params.put("newValue", "test2");

        JSInvoker function = new JSInvoker("function");
        Value value = new Value(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> newMap = function.apply("1", JsonUtils.toModifiableMap(value), params);

        assertEquals("test2", newMap.get("1"));
    }

    @Test
    public void testWithFunctionOnFile() throws Exception {
        Map<String, Object> params = new HashMap<String, Object>();
        String f = "function.js";
        params.put("function", f);

        JSInvoker function = new JSInvoker("function");
        Value value = new Value(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> newMap = function.apply("key", JsonUtils.toModifiableMap(value), params);

        assertEquals("f", newMap.get("result"));
    }

    @Test
    public void testWithFunctionOnFileWithRefresh() throws Exception {
        Map<String, Object> params = new HashMap<String, Object>();
        String f = "function.js";
        params.put("function", f);
        params.put("refresh", true);

        JSInvoker function = new JSInvoker("function");
        Value value = new Value(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> newMap = function.apply("1", JsonUtils.toModifiableMap(value), params);

        assertEquals("f", newMap.get("result"));
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

    @Test
    public void testWithAggregatorOnFile() throws Exception {
        List<Map<String, Object>> values = new LinkedList<Map<String, Object>>();
        values.add(new HashMap<String, Object>());

        Map<String, Object> params = new HashMap<String, Object>();
        String f = "aggregator.js";
        params.put("aggregator", f);

        JSInvoker function = new JSInvoker("aggregator");
        Map<String, Object> newMap = function.apply(values, params);

        assertEquals("a", newMap.get("result"));
    }

    @Test
    public void testWithAggregatorOnFileWithRefresh() throws Exception {
        List<Map<String, Object>> values = new LinkedList<Map<String, Object>>();
        values.add(new HashMap<String, Object>());

        Map<String, Object> params = new HashMap<String, Object>();
        String f = "aggregator.js";
        params.put("aggregator", f);
        params.put("refresh", true);

        JSInvoker function = new JSInvoker("aggregator");
        Map<String, Object> newMap = function.apply(values, params);

        assertEquals("a", newMap.get("result"));
    }

}
