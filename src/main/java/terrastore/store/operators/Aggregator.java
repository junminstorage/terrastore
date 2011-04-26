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
package terrastore.store.operators;

import java.util.List;
import java.util.Map;

/**
 * Interface to implement for aggregating values.
 *
 * @author Sergio Bossa
 */
public interface Aggregator {

    /**
     * Aggregate the given list of values, represented as a map of name -> value pairs, optionally taking into account given parameters,
     * and returning a map of resulting values.<br>
     * Maps can contain primitive values (such as integers, strings and alike), nested maps and lists of primitive and nested map values.
     *
     * @param values Values to aggregate.
     * @param parameters Optional parameters.
     * @return A map of aggregated values.
     * @throws {@link OperatorException} if something wrong happens during execution.
     */
    public Map<String, Object> apply(List<Map<String, Object>> values, Map<String, Object> parameters) throws OperatorException;
}
