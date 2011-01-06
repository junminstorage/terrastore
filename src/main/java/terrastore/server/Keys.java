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

package terrastore.server;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import terrastore.store.Key;

/**
 * @author Sven Johansson
 */
public class Keys extends AbstractSet<String> implements Serializable {

    private static final long serialVersionUID = 6257689309668637360L;

    private final Set<String> keys;
    
    public Keys(Set<String> keys) {
        this.keys = keys;
    }
    
    @Override
    public Iterator<String> iterator() {
        return keys.iterator();
    }

    @Override
    public int size() {
        return keys.size();
    }

    public static Keys fromKeySet(Set<Key> keySet) {
        Set<String> keys = new HashSet<String>();
        for (Key key : keySet) {
            keys.add(key.toString());
        }
        return new Keys(keys);
    }
}
