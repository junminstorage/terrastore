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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class Sets {

    public static <E> Set<E> hash(E... elements) {
        return com.google.common.collect.Sets.newHashSet(elements);
    }

    public static <E> Set<E> linked(E... elements) {
        return com.google.common.collect.Sets.newLinkedHashSet(Arrays.asList(elements));
    }

    public static <E> Set<E> serializing(Set<E> source) {
        return new SerializingSet<E>(source);
    }

    public static <E> Set<E> limited(Set<E> source, int limit) {
        return new LimitedSet<E>(source, limit);
    }

    public static <I, O> Set<O> transformed(Set<I> source, Transformer<I, O> transformer) {
        return new TransformedSet<I, O>(source, transformer);
    }

    public static <E> Set<E> union(List<Set<E>> sets) {
        return new UnionSet<E>(sets);
    }

    public static <E> Set<E> cons(E element, Set<E> set) {
        return com.google.common.collect.Sets.union(linked(element), set);
    }

    public static <E extends Comparable> Set<E> merge(Set<E> first, Set<E> second) {
        return new MergeSet<E>(first, second);
    }
}
