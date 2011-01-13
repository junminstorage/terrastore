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
package terrastore.store.conditions;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.store.operators.Condition;

/**
 * @author Sergio Bossa
 */
public class IfCondition implements Condition {

    private static final Logger LOG = LoggerFactory.getLogger(IfCondition.class);
    //
    private final static Map<String, Condition> CONDITIONS = new HashMap<String, Condition>();

    {
        CONDITIONS.put("absent", new Condition() {

            @Override
            public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
                return false;
            }

        });

        CONDITIONS.put("matches", new Condition() {

            @Override
            public boolean isSatisfied(String key, Map<String, Object> map, String expression) {
                String candidate = expression.substring(expression.indexOf('(') + 1, expression.indexOf(','));
                String test = expression.substring(expression.indexOf(',') + 1, expression.indexOf(')'));
                String value = map.get(candidate).toString();
                return value != null && value.equals(test);
            }

        });
    }

    @Override
    public boolean isSatisfied(String key, Map<String, Object> value, String expression) {
        try {
            String name = expression.substring(0, expression.indexOf('('));
            Condition actualCondition = CONDITIONS.get(name);
            if (actualCondition != null) {
                return actualCondition.isSatisfied(key, value, expression);
            } else {
                throw new IllegalStateException("Wrong condition expression: " + expression);
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new IllegalStateException("Wrong condition expression: " + expression);
        }
    }

}
