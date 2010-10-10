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

import com.google.common.collect.AbstractIterator;
import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class UnionSet<E> extends AbstractSet<E> {

    private final List<Set<E>> sets;

    UnionSet(List<Set<E>> sets) {
        this.sets = sets;
    }

    @Override
    public Iterator<E> iterator() {
        return new UnionIterator<E>();
    }

    @Override
    public int size() {
        UnionIterator iterator = new UnionIterator();
        int size = 0;
        while (iterator.hasNext()) {
            iterator.next();
            size++;
        }
        return size;
    }

    private class UnionIterator<E> extends AbstractIterator<E> {

        private int currentIndex = 0;
        private Set<E> computedElements = new HashSet<E>();
        private Iterator<E> currentIterator;

        @Override
        protected E computeNext() {
            while (true) {
                if (currentIterator == null || !currentIterator.hasNext()) {
                    currentIterator = null;
                    while (currentIndex < sets.size() && currentIterator == null) {
                        Set currentSet = sets.get(currentIndex);
                        if (currentSet.size() > 0) {
                            currentIterator = currentSet.iterator();
                        }
                        currentIndex++;
                    }
                }
                if (currentIterator != null && currentIterator.hasNext()) {
                    E element = currentIterator.next();
                    if (!computedElements.contains(element)) {
                        computedElements.add(element);
                        return element;
                    }
                } else {
                    return endOfData();
                }
            }
        }
    }
}
