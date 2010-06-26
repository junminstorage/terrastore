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
package terrastore.util.collect.parallel;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import jsr166y.ForkJoinPool;
import jsr166y.RecursiveAction;
import jsr166y.RecursiveTask;
import terrastore.util.collect.MergeSet;

/**
 * @author Sergio Bossa
 */
public class ParallelUtils {

    // TODO: make this configurable?
    private final static ForkJoinPool POOL = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
    //

    public static <E extends Comparable> Set<E> parallelMerge(List<Set<E>> sets) {
        ParallelMergeTask task = new ParallelMergeTask<E>(sets);
        POOL.execute(task);
        task.join();
        return task.getMerged();
    }

    public static <I, O, C extends Collection<O>> C parallelMap(List<I> input, MapCollector<O, C> collector, MapTask<I, O, C> mapper) {
        ParallelMapTask<I, O, C> task = new ParallelMapTask<I, O, C>(input, collector, mapper);
        POOL.execute(task);
        task.join();
        return task.getOutput();
    }

    private static class ParallelMergeTask<E extends Comparable> extends RecursiveAction {

        private final List<Set<E>> sets;
        private volatile Set<E> merged;

        public ParallelMergeTask(List<Set<E>> sets) {
            this.sets = sets;
        }

        @Override
        protected void compute() {
            if (sets.size() == 0) {
                merged = Collections.emptySet();
            } else if (sets.size() == 1) {
                merged = sets.get(0);
            } else if (sets.size() == 2) {
                Set<E> first = sets.get(0);
                Set<E> second = sets.get(1);
                merged = new MergeSet<E>(first, second);
            } else {
                int middle = sets.size() / 2;
                ParallelMergeTask t1 = new ParallelMergeTask(sets.subList(0, middle));
                ParallelMergeTask t2 = new ParallelMergeTask(sets.subList(middle, sets.size()));
                t1.fork();
                t2.fork();
                t1.join();
                t2.join();
                merged = new MergeSet<E>(t1.merged, t2.merged);
            }
        }

        public Set<E> getMerged() {
            return merged;
        }
    }

    private static class ParallelMapTask<I, O, C extends Collection<O>> extends RecursiveAction {

        private final List<I> input;
        private final MapCollector<O, C> collector;
        private final MapTask<I, O, C> mapper;
        private volatile C output;

        public ParallelMapTask(List<I> input, MapCollector<O, C> collector, MapTask<I, O, C> mapper) {
            this.input = input;
            this.collector = collector;
            this.mapper = mapper;
            this.output = collector.createCollector();
        }

        @Override
        protected void compute() {
            if (input.size() == 1) {
                output = mapper.map(input.iterator().next(), collector);
            } else if (input.size() > 1) {
                int middle = input.size() / 2;
                ParallelMapTask<I, O, C> t1 = new ParallelMapTask<I, O, C>(input.subList(0, middle), collector, mapper);
                ParallelMapTask<I, O, C> t2 = new ParallelMapTask<I, O, C>(input.subList(middle, input.size()), collector, mapper);
                t1.fork();
                t2.fork();
                t1.join();
                t2.join();
                output = collector.createCollector();
                output.addAll(t1.output);
                output.addAll(t2.output);
            } else {
                output = collector.createCollector();
            }
        }

        public C getOutput() {
            return output;
        }
    }
}
