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
package terrastore.server;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class Parameters extends AbstractMap<String, Object> implements Serializable {

    private static final long serialVersionUID = 12345678901L;

    private Map<String, Object> parameters;

    public Parameters(Map<String, Object> values) {
        this.parameters = values;
    }

    public Parameters() {
        this.parameters = new LinkedHashMap<String, Object>();
    }

    @Override
    public Object get(Object key) {
        return parameters.get(key);
    }

    @Override
    public Object put(String key, Object value) {
        return parameters.put(key, value);
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return parameters.entrySet();
    }
}
