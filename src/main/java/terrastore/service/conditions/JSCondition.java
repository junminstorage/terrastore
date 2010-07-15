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

import java.util.Map;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.store.operators.Condition;
import terrastore.util.json.JsonUtils;

/**
 * {@link terrastore.store.operators.Condition} implementation evaluating a JavaScript conditional expression over the key or value object.
 * <br><br>
 * Here is an example of conditional expression evaluated over the key:
 * <pre>
 * {@code
 * key == '123'
 * }
 * </pre>
 * And here is an example over the value object:
 * <pre>
 * {@code
 * value.'id' == '123'
 * }
 * </pre>
 *
 * @author Giuseppe Santoro
 * @author Sergio Bossa
 */
public class JSCondition implements Condition {

    private static final long serialVersionUID = 12345678901L;
    private static final Logger LOG = LoggerFactory.getLogger(JSCondition.class);
    private static final String WRAPPER = ""
            + "   function wrapper(key, value) { "
            + "      if(#condition#) {"
            + "         return true;"
            + "      }"
            + "      return false; "
            + "   }";
    //
    private final ScriptEngine engine;
    private volatile IllegalStateException exception;

    public JSCondition() {
        engine = new ScriptEngineManager().getEngineByName("JavaScript");
        try {
            if (engine == null) {
                exception = new IllegalStateException("No JavaScript engine found.");
            } else if (!engine.getFactory().getParameter("THREADING").equals("MULTITHREADED")) {
                exception = new IllegalStateException("The JavaScript engine is not thread-safe.");
            }
        } catch (Exception ex) {
            exception = new IllegalStateException("Error in script execution.", ex);
        }
    }

    @Override
    public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
        if (exception == null) {
            try {
                engine.eval(WRAPPER.replaceFirst("#condition#", expression));
                return (Boolean) ((Invocable) engine).invokeFunction(
                        "wrapper",
                        key,
                        engine.eval("(" + JsonUtils.fromMap(value).toString() + ")"));
            } catch (Exception ex) {
                LOG.error("Error in script execution.", ex);
                throw new IllegalStateException("Error in script execution.", ex);
            }
        } else {
            throw exception;
        }
    }
}
