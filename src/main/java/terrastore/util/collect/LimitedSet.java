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
import java.util.Iterator;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class LimitedSet<E> extends AbstractSet<E> {

    private final Set<E> sourceSet;
    private final int limit;

    public LimitedSet(Set<E> sourceSet, int limit) {
        this.sourceSet = sourceSet;
        this.limit = limit;
    }

    @Override
    public Iterator<E> iterator() {
        return new LimitedIterator(sourceSet.iterator());
    }

    @Override
    public int size() {
        if (limit > 0) {
            return sourceSet.size() < limit ? sourceSet.size() : limit;
        } else {
            return sourceSet.size();
        }
    }

    private class LimitedIterator extends AbstractIterator<E> {

        private int index = 0;
        private Iterator<E> sourceIterator;

        public LimitedIterator(Iterator<E> sourceIterator) {
            this.sourceIterator = sourceIterator;
        }

        @Override
        protected E computeNext() {
            if ((limit == 0 || index < limit) && sourceIterator.hasNext()) {
                index++;
                return sourceIterator.next();
            } else {
                return endOfData();
            }
        }
    }
}
