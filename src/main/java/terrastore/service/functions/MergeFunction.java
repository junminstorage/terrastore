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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import terrastore.store.operators.Function;

/**
 * @author Sven Johansson
 */
@SuppressWarnings("serial")
public class MergeFunction implements Function {

    @Override
    public Map<String, Object> apply(String key, Map<String, Object> value, Map<String, Object> parameters) {
        return copyFields(value, parameters);
    }
    
    private Map<String, Object> copyFields(Map<String, Object> value, Map<String, Object> newValues) {
        Map<String, Object> result = getMap(value);
        
        for (Entry<String, Object> entry : newValues.entrySet()) {
            if (isMap(entry) && isMap(value.get(entry.getKey()))) {
                copyEmbeddedFields(value, entry);
            } else {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        
        return result;
    }
    
    private void copyEmbeddedFields(Map<String, Object> value, Entry<String, Object> entry) {
        Map<String, Object> embeddedValue = (Map<String, Object>) value.get(entry.getKey());
        Map<String, Object> embeddedNewValues = (Map<String, Object>) entry.getValue();
        copyFields(embeddedValue, embeddedNewValues);
    }

    private boolean isMap(Entry<String, Object> counterEntry) {
        return Map.class.isAssignableFrom(counterEntry.getValue().getClass());    
    }
    
    private boolean isMap(Object object) {
        return object != null && Map.class.isAssignableFrom(object.getClass());
    }

    private Map<String, Object> getMap(Map<String, Object> map) {
        if (null == map) {
            return new HashMap<String, Object>();
        }
        return map;
    }

    
}
