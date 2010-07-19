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
public class AtomicCounterFunction implements Function {

    private static final long serialVersionUID = -1148253663584090093L;

    @Override
    public Map<String, Object> apply(String key, Map<String, Object> value, Map<String, Object> parameters) {
        return applyCounters(value, parameters);
    }

    /**
     * Introspects the parameter map and applies any counters encountered. 
     * 
     * @param value The original object/map
     * @param counters A map of function parameters/counters
     * @return
     */
    private Map<String, Object> applyCounters(final Map<String, Object> value, final Map<String, Object> counters) {
        Map<String, Object> result = getMap(value);
        
        for (Entry<String, Object> counterEntry : counters.entrySet()) {
            if (isMap(counterEntry)) {
                applyToEmbeddedMap(result, counterEntry);
            } else if (isValue(counterEntry)) {
                applyCounter(result, counterEntry);
            }
        }
        
        return result;
    }
    
    /**
     * Applies a counter/modifier to a field of an object.
     * 
     * @param value The original object
     * @param entry The function parameter Entry describing the desired modification to a particular field.
     */
    private void applyCounter(Map<String, Object> value, Entry<String, Object> entry) {
        int originalValue = getOriginalValue(value.get(entry.getKey()));
        int modifier = getModifier((String) entry.getValue());
        value.put(entry.getKey(), String.valueOf(originalValue + modifier));
    }
    
    /**
     * Examines Maps embedded in the function parameters and applies counters to embedded objects.
     * 
     * @param value The original object
     * @param counterEntry The entry of the function parameter map that contains an embedded map.
     */
    @SuppressWarnings("unchecked")
    private void applyToEmbeddedMap(Map<String, Object> value, Entry<String, Object> counterEntry) {
        Object embeddedValue = value.get(counterEntry.getKey());
        if (null != embeddedValue && !isMap(embeddedValue)) {
            throw new IllegalArgumentException("The object at path '" + counterEntry.getKey() + "' is not an embedded object.");
        }
        Map<String, Object> embeddedObject = (Map<String, Object>) embeddedValue;
        Map<String, Object> embeddedCounter = (Map<String, Object>) counterEntry.getValue();
        
        Map<String, Object> result = applyCounters(embeddedObject, embeddedCounter);
        if (!result.isEmpty()) {
            value.put(counterEntry.getKey(), result);
        }
    }
    
    /**
     * Returns the integer value of a counter parameter.
     * 
     * @param stringValue The String representation of the counter/modifier.
     * 
     * @return The modifier as int.
     */
    private int getModifier(String stringValue) {
        try {
            return Integer.parseInt(stringValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("The value '" + stringValue + "' cannot be applied as a counter. The value must be a positive or negative integer.");
        }
    }
    
    /**
     * The original value that a counter would be applied to.
     * If the original value does not exist, the integer value will be interpreted as 0.
     * 
     * @param original The value to be interpreted as an original integer value.
     * 
     * @return The integer representation of a value. Zero if it does not exist.
     */
    private int getOriginalValue(Object original) {
        if (null == original) {
            return 0;
        }
        if (Map.class.isAssignableFrom(original.getClass())) {
            throw new IllegalArgumentException("Cannot apply counter to embedded object.");
        }
        try {
            return Integer.parseInt((String) original);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot apply counter to value '" + original + "'.");
        }
    }

    private boolean isMap(Entry<String, Object> counterEntry) {
        return Map.class.isAssignableFrom(counterEntry.getValue().getClass());    
    }
    
    private boolean isMap(Object object) {
        return Map.class.isAssignableFrom(object.getClass());
    }
    
    private boolean isValue(Entry<String, Object> counterEntry) {
        return counterEntry.getValue() instanceof String;
    }

    private Map<String, Object> getMap(Map<String, Object> map) {
        if (null == map) {
            return new HashMap<String, Object>();
        }
        return map;
    }

    
}
