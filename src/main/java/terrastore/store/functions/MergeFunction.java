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
package terrastore.store.functions;

import java.util.Map;
import terrastore.store.ValidationException;
import terrastore.store.Value;
import terrastore.store.internal.InPlace;

import terrastore.store.operators.Function;
import terrastore.util.json.JsonUtils;

/**
 * @author Sven Johansson
 * @author Sergio Bossa
 */
public class MergeFunction implements InPlace, Function {

    @Override
    public Map<String, Object> apply(String key, Map<String, Object> value, Map<String, Object> parameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value apply(String key, Value value, Map<String, Object> updates) {
        try {
            return JsonUtils.merge(value, updates);
        } catch (ValidationException ex) {
            throw new IllegalStateException(ex.getErrorMessage().getMessage(), ex);
        }
    }

}
