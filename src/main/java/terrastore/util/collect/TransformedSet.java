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
package terrastore.util.collect;

import com.google.common.collect.AbstractIterator;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class TransformedSet<I, O> extends AbstractSet<O> {

    private final Set<I> sourceSet;
    private final Transformer<I, O> transformer;

    public TransformedSet(Set<I> sourceSet, Transformer<I, O> transformer) {
        this.sourceSet = sourceSet;
        this.transformer = transformer;
    }

    @Override
    public Iterator<O> iterator() {
        return new TransformerIterator<I, O>(sourceSet.iterator(), transformer);
    }

    @Override
    public int size() {
        return sourceSet.size();
    }

    private class TransformerIterator<I, O> extends AbstractIterator<O> {

        private final Iterator<I> sourceIterator;
        private final Transformer<I, O> transformer;

        public TransformerIterator(Iterator<I> sourceIterator, Transformer<I, O> transformer) {
            this.sourceIterator = sourceIterator;
            this.transformer = transformer;
        }

        @Override
        protected O computeNext() {
            if (sourceIterator.hasNext()) {
                return transformer.transform(sourceIterator.next());
            } else {
                return endOfData();
            }
        }
    }
}
