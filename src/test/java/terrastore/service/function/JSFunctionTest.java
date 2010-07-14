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
package terrastore.service.function;

import org.junit.Test;
import terrastore.service.functions.JSFunction;
import terrastore.store.types.JsonValue;
import terrastore.util.json.JsonUtils;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Giuseppe Santoro
 */
public class JSFunctionTest {
    private static final String JSON_VALUE = "{\"test\":\"test\"}";

    @Test
    public void testSatisfiedWithJsonValue() throws Exception {
        Map<String, Object> params = new HashMap<String, Object>();
        String f = "function " + JSFunction.UPDATE_FUNCTION + "(key, value) {" +
                                "   if(value['test'] == 'test')" +
                                "       value['test'] = 'newtest';" +
                                "   return value;" +
                                "}";
        params.put(JSFunction.UPDATE_FUNCTION, f);
        JSFunction function = new JSFunction();
        JsonValue value = new JsonValue(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> newMap = function.apply("key", JsonUtils.toModifiableMap(value), params);

        assertEquals("newtest", newMap.get("test"));
    }

}