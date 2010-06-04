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

import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class MergeSet<E extends Comparable> extends AbstractSet<E> {

    private final Set<E> mergedSet;

    public MergeSet(Set<E> first, Set<E> second) {
        this.mergedSet = merge(first, second);
    }

    @Override
    public Iterator<E> iterator() {
        return mergedSet.iterator();
    }

    @Override
    public int size() {
        return mergedSet.size();
    }

    private Set<E> merge(Set<E> first, Set<E> second) {
        Set<E> result = new LinkedHashSet<E>();
        Iterator<E> iIt = first.iterator();
        Iterator<E> jIt = second.iterator();
        if (!iIt.hasNext() && !jIt.hasNext()) {
            return Collections.EMPTY_SET;
        } else if (iIt.hasNext() && !jIt.hasNext()) {
            return first;
        } else if (!iIt.hasNext() && jIt.hasNext()) {
            return second;
        } else {
            E iV = iIt.next();
            E jV = jIt.next();
            E pivot = null;
            E current = null;
            Iterator<E> pivotScroller = null;
            Iterator<E> currentScroller = null;
            Iterator<E> swapper = null;
            if (iV.compareTo(jV) < 0) {
                pivot = jV;
                current = iV;
                pivotScroller = jIt;
                currentScroller = iIt;
            } else {
                pivot = iV;
                current = jV;
                pivotScroller = iIt;
                currentScroller = jIt;
            }
            while (true) {
                result.add(current);
                if (currentScroller.hasNext()) {
                    while (currentScroller.hasNext()) {
                        E candidate = currentScroller.next();
                        if (candidate.compareTo(pivot) < 0) {
                            result.add(candidate);
                        } else {
                            current = pivot;
                            pivot = candidate;
                            swapper = pivotScroller;
                            pivotScroller = currentScroller;
                            currentScroller = swapper;
                            break;
                        }
                    }
                } else {
                    result.add(pivot);
                    while (pivotScroller.hasNext()) {
                        result.add(pivotScroller.next());
                    }
                    break;
                }
            }
            return result;
        }
    }
}
