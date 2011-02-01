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
package terrastore.store.functions.support;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Sven Johansson
 */
public class MapUtil {
    
    public static Map<String, Object> map(MapEntry... entries) {
        Map<String, Object> map = new HashMap<String, Object>();
        for (MapEntry entry : entries) {
            map.put(entry.key(), entry.value());
        }
        return map;
    }
    
    public static MapEntry entry(String key, Object value) {
        return new MapEntry(key, value);
    }
    
    public static class MapEntry {
        
        private final String key;
        private final Object value;

        public MapEntry(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public Object value() {
            return value;
        }

        public String key() {
            return key;
        }
    }
    
}
