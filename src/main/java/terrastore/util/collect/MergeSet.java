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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class MergeSet<E extends Comparable> extends AbstractSet<E> {

    private final List<E> merged;

    public MergeSet(Set<E> first, Set<E> second) {
        this.merged = merge(first, second);
    }

    @Override
    public Iterator<E> iterator() {
        return merged.iterator();
    }

    @Override
    public int size() {
        return merged.size();
    }

    private List<E> merge(Set<E> first, Set<E> second) {
        List<E> result = new ArrayList<E>(first.size() + second.size());
        Iterator<E> iIt = first.iterator();
        Iterator<E> jIt = second.iterator();
        if (!iIt.hasNext() && !jIt.hasNext()) {
            return Collections.EMPTY_LIST;
        } else if (iIt.hasNext() && !jIt.hasNext()) {
            return new ArrayList<E>(first);
        } else if (!iIt.hasNext() && jIt.hasNext()) {
            return new ArrayList<E>(second);
        } else {
            int index = 0;
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
            index = addToMerged(index, current, result);
            while (currentScroller.hasNext()) {
                E candidate = currentScroller.next();
                if (candidate.compareTo(pivot) < 0) {
                    index = addToMerged(index, candidate, result);
                } else {
                    index = addToMerged(index, pivot, result);
                    current = pivot;
                    pivot = candidate;
                    swapper = pivotScroller;
                    pivotScroller = currentScroller;
                    currentScroller = swapper;
                }
            }
            index = addToMerged(index, pivot, result);
            while (pivotScroller.hasNext()) {
                index = addToMerged(index, pivotScroller.next(), result);
            }
        }
        return result;
    }

    private int addToMerged(int index, E candidate, List<E> merged) {
        if (index == 0 || !merged.get(index - 1).equals(candidate)) {
            merged.add(index, candidate);
            return index + 1;
        } else {
            return index;
        }
    }
}
