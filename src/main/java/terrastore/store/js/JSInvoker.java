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

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.List;
import terrastore.store.operators.Function;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.codehaus.jackson.map.ObjectMapper;
import terrastore.store.operators.Aggregator;
import terrastore.util.io.IOUtils;
import static terrastore.startup.Constants.*;

/**
 * Invoke Javascript functions to implement {@link terrastore.store.operators.Aggregator}s and {@link terrastore.store.operators.Function}s on-the-fly.
 * This implementation evaluates a JavaScript function passed as a client parameter whose name/key is configured in the class constructor.
 * <br>
 * <br>
 * In order to work as a {@link terrastore.store.operators.Aggregator}, the function must accept the following two parameters and return a json object:
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
 * In order to work as a {@link terrastore.store.operators.Function}, the function must accept the following three parameters and return a json object:
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

    private static final Logger LOG = LoggerFactory.getLogger(JSInvoker.class);
    //
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    //
    private static final String JS_SUFFIX = ".js";
    private static final String FUNCTION_PREFIX = "function";
    private static final String REFRESH = "refresh";
    private static final String AGGREGATOR_WRAPPER = ""
            + "function invokeAggregator(fn, values, params) { "
            + "   return JSON.stringify(fn(values, params)); "
            + "}";
    private static final String FUNCTION_WRAPPER = ""
            + "function invokeFunction(fn, key, value, params) { "
            + "   return JSON.stringify(fn(key, value, params)); "
            + "}";
    //
    private static final ScriptEngine ENGINE;
    private static IllegalStateException EXCEPTION;

    static {
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
    //

    private final String name;
    private final ConcurrentMap<String, String> fnCache;

    public JSInvoker(String name) {
        this.name = name;
        this.fnCache = new ConcurrentHashMap<String, String>();
    }

    @Override
    public Map<String, Object> apply(List<Map<String, Object>> values, Map<String, Object> parameters) {
        if (EXCEPTION == null) {
            try {
                boolean refresh = hasRefresh(parameters);
                String fn = getFunction(parameters.get(name).toString(), refresh);
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
            } catch (IOException ex) {
                LOG.error(ex.getMessage(), ex);
                throw new IllegalStateException(ex.getMessage(), ex);
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
                boolean refresh = hasRefresh(parameters);
                String fn = getFunction(parameters.get(name).toString(), refresh);
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
            } catch (IOException ex) {
                LOG.error(ex.getMessage(), ex);
                throw new IllegalStateException(ex.getMessage(), ex);
            } catch (Exception ex) {
                LOG.error("Error in script execution.", ex);
                throw new IllegalStateException("Error in script execution.", ex);
            }
        } else {
            throw EXCEPTION;
        }
    }

    private boolean hasRefresh(Map<String, Object> parameters) {
        return parameters.get(REFRESH) != null ? Boolean.parseBoolean(parameters.get(REFRESH).toString()) : false;
    }

    private String getFunction(String declaration, boolean refresh) throws IOException {
        String fn = null;
        if (isDeclaredOnFile(declaration)) {
            if (!fnCache.containsKey(declaration) || refresh) {
                fn = loadFunction(declaration);
                fnCache.putIfAbsent(declaration, fn);
            } else {
                fn = fnCache.get(declaration);
            }
        } else if (isDeclaredInLine(declaration)) {
            fn = declaration;
        } else {
            throw new IllegalArgumentException("Bad function declaration: " + declaration);
        }
        return fn;
    }

    private boolean isDeclaredInLine(String declaration) {
        return declaration.startsWith(FUNCTION_PREFIX);
    }

    private boolean isDeclaredOnFile(String declaration) {
        return declaration.endsWith(JS_SUFFIX);
    }

    private String loadFunction(String declaration) throws IOException {
        String separator = System.getProperty("file.separator");
        File f = IOUtils.getFileFromTerrastoreHome(JAVASCRIPT_DIR + separator + declaration);
        return Files.toString(f, Charset.forName("UTF-8"));
    }

}
