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

import java.util.Map;
import java.util.Map.Entry;

import terrastore.store.operators.Function;

/**
 * @author Sven Johansson
 * @author Sergio Bossa
 */
public class MergeFunction implements Function {

    private static final long serialVersionUID = 12345678901L;

    @Override
    public Map<String, Object> apply(String key, Map<String, Object> value, Map<String, Object> parameters) {
        mergeFields(parameters, value);
        return value;
    }

    private void mergeFields(Map<String, Object> source, Map<String, Object> destination) {
        for (Entry<String, Object> sourceEntry : source.entrySet()) {
            if (isMap(sourceEntry.getValue()) && isMap(destination.get(sourceEntry.getKey()))) {
                mergeFields((Map<String, Object>) sourceEntry.getValue(), (Map<String, Object>) destination.get(sourceEntry.getKey()));
            } else {
                destination.put(sourceEntry.getKey(), sourceEntry.getValue());
            }
        }
    }

    private boolean isMap(Object object) {
        return object != null && Map.class.isAssignableFrom(object.getClass());
    }
}
