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
package terrastore.service.functions;

import org.junit.Test;
import terrastore.store.types.JsonValue;
import terrastore.util.json.JsonUtils;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

/**
 * @author Giuseppe Santoro
 * @author Sergio Bossa
 */
public class JSFunctionTest {

    private static final String JSON_VALUE = "{\"test\":\"test\"}";

    @Test
    public void testWithUpdateFunction() throws Exception {
        Map<String, Object> params = new HashMap<String, Object>();
        String f = "function " + JSFunction.FUNCTION_NAME + "(key, value, params) {"
                + "   if(value['test'] == 'test')"
                + "       value['test'] = params.newValue;"
                + "   return value;"
                + "}";
        params.put(JSFunction.FUNCTION_NAME, f);
        params.put("newValue", "test2");

        JSFunction function = new JSFunction();
        JsonValue value = new JsonValue(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> newMap = function.apply("key", JsonUtils.toModifiableMap(value), params);

        assertEquals("test2", newMap.get("test"));
    }

    @Test
    public void testWithMoreUpdateFunctionsHavingTheSameName() throws Exception {
        Map<String, Object> params = new HashMap<String, Object>();
        String f1 = "function " + JSFunction.FUNCTION_NAME + "(key, value, params) {"
                + "   if(value['test'] == 'test')"
                + "       value['test'] = 'test2';"
                + "   return value;"
                + "}";
        String f2 = "function " + JSFunction.FUNCTION_NAME + "(key, value, params) {"
                + "   if(value['test'] == 'test')"
                + "       value['test'] = 'test3';"
                + "   return value;"
                + "}";

        params.put(JSFunction.FUNCTION_NAME, f1);

        JSFunction function = new JSFunction();
        JsonValue value = new JsonValue(JSON_VALUE.getBytes("UTF-8"));
        Map<String, Object> newMap = function.apply("key", JsonUtils.toModifiableMap(value), params);

        assertEquals("test2", newMap.get("test"));

        params.clear();

        params.put(JSFunction.FUNCTION_NAME, f2);

        newMap = function.apply("key", JsonUtils.toModifiableMap(value), params);

        assertEquals("test3", newMap.get("test"));
    }
}
