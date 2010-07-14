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
package terrastore.service.conditions;

import java.text.MessageFormat;
import java.util.Map;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import terrastore.store.operators.Condition;
import terrastore.util.json.JsonUtils;

/**
 * {@link terrastore.store.operators.Condition} implementation evaluating JS expression(function)
 * This class will evaluate JS expression, please see above.<br>
 * Expression Example:
 * <pre>
 * {@code
 * value.'id' == '123'
 * }
 * </pre>
 *
 * @author Giuseppe Santoro
 */
public class JSCondition implements Condition {
    
    private static final long serialVersionUID = 6861326254722412197L;
    private static final Logger LOG = LoggerFactory.getLogger(JSCondition.class);
    private static final String WRAPPER = "" +
            "   function wrapper(key, value) { " +
            "      if( %1$2s ) {" +
            "         return true;" +
            "      }" +
            "      return false; " +
            "   }";


    @Override
    public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
        LOG.debug(MessageFormat.format("key:{0} | value:{1} | expression:{2}", key, value, expression));
        ScriptEngineManager mgr = new ScriptEngineManager();
        ScriptEngine engine = mgr.getEngineByName("JavaScript");
        try {
            Object valueNO = engine.eval("(" + JsonUtils.fromMap(value).toString() + ")");
            engine.eval(String.format(WRAPPER, expression));
            Invocable inv = (Invocable) engine;
            return (Boolean) inv.invokeFunction("wrapper", key, valueNO);
            
        } catch (ScriptException e) {
            LOG.error("Error in script execution.", e);
            throw new IllegalArgumentException("Error in script execution.", e);

        } catch (NoSuchMethodException e) {
            LOG.error("NoSuchMethodException", e);
            throw new IllegalArgumentException("NoSuchMethodException", e);

        }
    }
}