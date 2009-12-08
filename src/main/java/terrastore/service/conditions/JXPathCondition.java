/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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

import java.util.List;
import java.util.Map;
import org.apache.commons.jxpath.JXPathContext;
import terrastore.store.operators.Condition;

/**
 * {@link terrastore.store.operators.Condition} implementation evaluating JXPath expressions
 * (see http://commons.apache.org/jxpath).
 *
 * @author Sergio Bossa
 */
public class JXPathCondition implements Condition {

    private static final long serialVersionUID = 12345678901L;

    @Override
    public boolean isSatisfied(Map<String, Object> value, String expression) {
        JXPathContext context = JXPathContext.newContext(value);
        context.setLenient(true);
        List selection = context.selectNodes(expression);
        return selection != null & selection.size() > 0;
    }
}
