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

import terrastore.store.operators.Function;
import terrastore.store.Value;
import terrastore.util.json.JsonUtils;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileReader;
import java.util.Map;

/**
 * {@link terrastore.store.operators.Function} implementation evaluating a JavaScript function passed as a parameter named {@link #FUNCTION_NAME}.<br>
 * The name of the JavaScript function itself must be equal to {@link #FUNCTION_NAME}, and must accept three parameters:
 * <ul>
 * <li>The key of the value to update.</li>
 * <li>The value to update</li>
 * <li>The user-defined parameters map.</li>
 * </ul>
 * Here is an example:
 * <pre>
 * {@code
 * function update(key, value, params) {
 *     value.something = params.newValue;
 *     return value;
 * }
 * </pre>
 * 
 * @author Giuseppe Santoro
 * @author Sergio Bossa
 */
public class JSFunction implements Function {

    public static final String FUNCTION_NAME = "update";
    //
    private static final long serialVersionUID = 12345678901L;
    private static final Logger LOG = LoggerFactory.getLogger(JSFunction.class);
    private static final String WRAPPER = ""
            + "function wrapper(update, key, value, params) { "
            + "   return JSON.stringify(update(key, value, params)); "
            + "}";
    //
    private static ScriptEngine ENGINE;
    private static IllegalStateException EXCEPTION;
    {
        ENGINE = new ScriptEngineManager().getEngineByName("JavaScript");
        try {
            if (ENGINE != null && ENGINE.getFactory().getParameter("THREADING").equals("MULTITHREADED")) {
                ENGINE.eval(new FileReader(this.getClass().getClassLoader().getResource("json.js").getFile()));
                ENGINE.eval(WRAPPER);
            } else if (ENGINE == null) {
                EXCEPTION = new IllegalStateException("No JavaScript engine found.");
            } else {
                EXCEPTION = new IllegalStateException("The JavaScript engine is not thread-safe.");
            }
        } catch (Exception ex) {
            EXCEPTION = new IllegalStateException("Error in script execution.", ex);
        }
    }

    public Map<String, Object> apply(String key, Map<String, Object> value, Map<String, Object> parameters) {
        if (EXCEPTION == null) {
            try {
                Object result = ((Invocable) ENGINE).invokeFunction("wrapper",
                        ENGINE.eval("(" + parameters.remove(FUNCTION_NAME).toString() + ")"),
                        key,
                        ENGINE.eval("(" + JsonUtils.fromMap(value).toString() + ")"),
                        ENGINE.eval("(" + JsonUtils.fromMap(parameters).toString() + ")"));
                return JsonUtils.toUnmodifiableMap(new Value(result.toString().getBytes()));
            } catch (Exception ex) {
                LOG.error("Error in script execution.", ex);
                throw new IllegalStateException("Error in script execution.", ex);
            }
        } else {
            throw EXCEPTION;
        }
    }
}
