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
import terrastore.store.types.JsonValue;
import terrastore.util.json.JsonUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.Map;

/**
 * {@link terrastore.store.operators.Function} implementation evaluating JS function,<br>
 * you can access key and actual value as parameter function, the order is:<br>
 * <ul>
 * <li>1-param)key</li>
 * <li>2-param)value</li>
 * <li>3-param)parameters</li>
 * </ul>
 * attention please to set function name with {@link #UPDATE_FUNCTION}. then return the modified map otherwise
 * will be thrown an {@link IllegalStateException}.<br>
 * Function Example:
 * 
 * <pre>
 * {@code
 * function updateFunction(key, value, params) {
 *     value.id = 'new value';
 *     return value;
 * }
 * }
 * </pre>
 * 
 * @author Giuseppe Santoro
 */
public class JSFunction implements Function {
    
    public static final String UPDATE_FUNCTION = "updateFunction";
    
    private static final long serialVersionUID = -2772030179595908762L;
    private static final Logger LOG = LoggerFactory.getLogger(JSFunction.class);
    private static final String WRAPPER = "" + 
                                          "function wrapper(key, value, params) { " + 
                                          "   return JSON.stringify(" + UPDATE_FUNCTION + "(key, value, params)); " + 
                                          "}";

    public Map<String, Object> apply(String key, Map<String, Object> value, Map<String, Object> parameters) {
        LOG.debug(MessageFormat.format("key:{0} | value:{1} | parameters:{2}", key, value, parameters));
        ScriptEngineManager mgr = new ScriptEngineManager();
        ScriptEngine engine = mgr.getEngineByName("JavaScript");
        try {
            engine.eval(new FileReader(this.getClass().getClassLoader().getResource("json.js").getFile()));
            engine.eval(parameters.get(UPDATE_FUNCTION).toString());
            Object valueNO = engine.eval("(" + JsonUtils.fromMap(value).toString() + ")");
            Object paramsNO = engine.eval("(" + JsonUtils.fromMap(parameters).toString() + ")");
            engine.eval(WRAPPER);
            Invocable inv = (Invocable) engine;
            Object o = inv.invokeFunction("wrapper", key, valueNO, paramsNO);
            return JsonUtils.toUnmodifiableMap(new JsonValue(o.toString().getBytes("UTF-8")));

        } catch (ScriptException e) {
            LOG.error("Error in script execution.", e);
            throw new IllegalArgumentException("Error in script execution.", e);

        } catch (NoSuchMethodException e) {
            LOG.error("NoSuchMethodException", e);
            throw new IllegalArgumentException("NoSuchMethodException", e);

        } catch (FileNotFoundException e) {
            LOG.error("Json.js not found.", e);
            throw new IllegalStateException("Json.js not found.", e);

        } catch (UnsupportedEncodingException e) {
            LOG.error("UnsupportedEncodingException", e);
            throw new IllegalStateException("UnsupportedEncodingException", e);

        }

    }
}