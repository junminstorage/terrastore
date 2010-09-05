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
package terrastore.util.collect;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class SerializingSet<E> extends AbstractSet<E> implements Serializable {

    private Set<E> source;

    public SerializingSet(Set<E> source) {
        this.source = source;
    }

    @Override
    public Iterator<E> iterator() {
        return source.iterator();
    }

    @Override
    public int size() {
        return source.size();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        int size = source.size();
        out.writeInt(size);
        for (E element : source) {
            out.writeObject(element);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException {
        try {
            int size = in.readInt();
            source = new LinkedHashSet<E>();
            for (int i = 0; i < size; i++) {
                source.add((E) in.readObject());
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
