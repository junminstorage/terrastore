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

import java.util.List;
import terrastore.store.operators.Function;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStreamReader;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import terrastore.store.operators.Aggregator;

/**
 * Invoke Javascript functions to implement {@link terrastore.store.operators.Aggregator}s and {@link terrastore.store.operators.Function}s on-the-fly.
 * This implementation evaluates a JavaScript function passed as a client parameter named "js".
 * <br>
 * <br>
 * In order to work as a {@link terrastore.store.operators.Aggregator}, the function must be passed as a client parameter named "function",
 * and accept the following two parameters and return a json object:
 * <ul>
 * <li>The values to aggregate.</li>
 * <li>The user-defined parameters map.</li>
 * </ul>
 * Here is an example:
 * <pre>
 * {@code
 * function(values, params) {
 *     ...
 * }
 * </pre>
 * <br>
 * In order to work as a {@link terrastore.store.operators.Function}, the function must be passed as a client parameter named "aggregator",
 * and accept the following three parameters and return a json object:
 * <ul>
 * <li>The key of the value to update.</li>
 * <li>The value to update</li>
 * <li>The user-defined parameters map.</li>
 * </ul>
 * Here is an example:
 * <pre>
 * {@code
 * function(key, value, params) {
 *     return {};
 * }
 * </pre>
 * 
 * @author Giuseppe Santoro
 * @author Sergio Bossa
 */
public class JSInvoker implements Aggregator, Function {

    public static final String FUNCTION_NAME = "function";
    public static final String AGGREGATOR_NAME = "aggregator";
    //
    private static final Logger LOG = LoggerFactory.getLogger(JSInvoker.class);
    //
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    //
    private static final String AGGREGATOR_WRAPPER = ""
            + "function invokeAggregator(fn, values, params) { "
            + "   return JSON.stringify(fn(values, params)); "
            + "}";
    private static final String FUNCTION_WRAPPER = ""
            + "function invokeFunction(fn, key, value, params) { "
            + "   return JSON.stringify(fn(key, value, params)); "
            + "}";
    //
    private static ScriptEngine ENGINE;
    private static IllegalStateException EXCEPTION;

    {
        ENGINE = new ScriptEngineManager().getEngineByName("JavaScript");
        try {
            if (ENGINE != null && ENGINE.getFactory().getParameter("THREADING").equals("MULTITHREADED")) {
                ENGINE.eval(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("json.js")));
                ENGINE.eval(AGGREGATOR_WRAPPER);
                ENGINE.eval(FUNCTION_WRAPPER);
            } else if (ENGINE == null) {
                EXCEPTION = new IllegalStateException("No JavaScript engine found.");
            } else {
                EXCEPTION = new IllegalStateException("The JavaScript engine is not thread-safe.");
            }
        } catch (Exception ex) {
            EXCEPTION = new IllegalStateException(ex.getMessage(), ex);
        }
    }

    @Override
    public Map<String, Object> apply(List<Map<String, Object>> values, Map<String, Object> parameters) {
        if (EXCEPTION == null) {
            try {
                Object fn = parameters.get(AGGREGATOR_NAME);
                if (fn != null) {
                    Object result = ((Invocable) ENGINE).invokeFunction("invokeAggregator",
                            ENGINE.eval("(" + fn.toString() + ")"),
                            ENGINE.eval("(" + JSON_MAPPER.writeValueAsString(values) + ")"),
                            ENGINE.eval("(" + JSON_MAPPER.writeValueAsString(parameters) + ")"));
                    return JSON_MAPPER.readValue(result.toString(), Map.class);
                } else {
                    throw new IllegalStateException("No aggregator provided in client parameters!");
                }
            } catch (IllegalStateException ex) {
                throw ex;
            } catch (Exception ex) {
                LOG.error("Error in script execution.", ex);
                throw new IllegalStateException("Error in script execution.", ex);
            }
        } else {
            throw EXCEPTION;
        }
    }

    public Map<String, Object> apply(String key, Map<String, Object> value, Map<String, Object> parameters) {
        if (EXCEPTION == null) {
            try {
                Object fn = parameters.get(FUNCTION_NAME);
                if (fn != null) {
                    Object result = ((Invocable) ENGINE).invokeFunction("invokeFunction",
                            ENGINE.eval("(" + fn.toString() + ")"),
                            key,
                            ENGINE.eval("(" + JSON_MAPPER.writeValueAsString(value) + ")"),
                            ENGINE.eval("(" + JSON_MAPPER.writeValueAsString(parameters) + ")"));
                    return JSON_MAPPER.readValue(result.toString(), Map.class);
                } else {
                    throw new IllegalStateException("No function provided in client parameters!");
                }
            } catch (IllegalStateException ex) {
                throw ex;
            } catch (Exception ex) {
                LOG.error("Error in script execution.", ex);
                throw new IllegalStateException("Error in script execution.", ex);
            }
        } else {
            throw EXCEPTION;
        }
    }

}
