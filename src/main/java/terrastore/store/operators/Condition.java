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
package terrastore.store.operators;

import java.io.Serializable;
import java.util.Map;

/**
 * Interface to implement for evaluating conditions on bucket values.
 *
 * @author Sergio Bossa
 */
public interface Condition extends Serializable {

    /**
     * Evaluate this condition on the given value, represented as a map of name -> value pairs (associative array).
     *
     * @param value The value to evaluate condition on.
     * @param expression The condition expression.
     * @return True if satisfied, false otherwise.
     */
    public boolean isSatisfied(Map<String, Object> value, String expression);
}
