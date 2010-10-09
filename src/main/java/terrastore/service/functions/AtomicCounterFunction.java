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
public class AtomicCounterFunction implements Function {

    @Override
    public Map<String, Object> apply(String key, Map<String, Object> value, Map<String, Object> parameters) {
        return applyCounters(value, parameters);
    }

    private Map<String, Object> applyCounters(Map<String, Object> counters, Map<String, Object> modifiers) {
        for (Entry<String, Object> modifier : modifiers.entrySet()) {
            Object counter = counters.get(modifier.getKey());
            if (isMap(counter) && isMap(modifier.getValue())) {
                counters.put(modifier.getKey(), applyCounters((Map<String, Object>) counter, (Map<String, Object>) modifier.getValue()));
            } else if (isValue(counter) && isValue(modifier.getValue())) {
                counters.put(modifier.getKey(), applyCounter(counter, modifier.getValue()));
            } else if (counter == null) {
                counters.put(modifier.getKey(), modifier.getValue());
            } else {
                throw new IllegalArgumentException("Different types: " + counter.getClass().getName() + " and " + modifier.getValue().getClass().getName());
            }
        }
        return counters;
    }

    private Object applyCounter(Object counter, Object modifier) {
        Object result = null;
        try {
            if (counter instanceof String && modifier instanceof String) {
                result = Long.toString(Long.parseLong((String) counter) + Long.parseLong((String) modifier));
            } else if (counter instanceof Number && modifier instanceof Number) {
                result = ((Number) counter).longValue() + ((Number) modifier).longValue();
            } else {
                throw new IllegalArgumentException("Different types: " + counter.getClass().getName() + " and " + modifier.getClass().getName());
            }
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(ex.getMessage());
        }
        return result;
    }

    private boolean isMap(Object object) {
        return object != null && Map.class.isAssignableFrom(object.getClass());
    }

    private boolean isValue(Object object) {
        return object != null && !Map.class.isAssignableFrom(object.getClass());
    }
}
