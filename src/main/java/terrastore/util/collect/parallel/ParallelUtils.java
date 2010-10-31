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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import jsr166y.ForkJoinPool;
import jsr166y.RecursiveAction;
import terrastore.service.QueryOperationException;
import terrastore.util.collect.MergeSet;

/**
 * @author Sergio Bossa
 */
public class ParallelUtils {

    public static <E extends Comparable> Set<E> parallelMerge(List<Set<E>> sets, ForkJoinPool fjPool) throws ParallelExecutionException {
        ParallelMergeTask task = new ParallelMergeTask<E>(sets);
        fjPool.execute(task);
        task.join();
        return task.getMerged();
    }

    public static <I, O, C extends Collection> C parallelMap(final Collection<I> input, final MapTask<I, O> mapper, final MapCollector<O, C> collector, ExecutorService executor) throws ParallelExecutionException, QueryOperationException {
        try {
            List<Callable<O>> tasks = new ArrayList<Callable<O>>(input.size());
            for (final I current : input) {
                tasks.add(new Callable<O>() {

                    @Override
                    public O call() throws QueryOperationException {
                        return mapper.map(current);
                    }
                });
            }
            List<Future<O>> results = executor.invokeAll(tasks);
            List<O> outputs = new ArrayList<O>(results.size());
            for (Future<O> result : results) {
                outputs.add(result.get());
            }
            return collector.collect(outputs);
        } catch (ExecutionException ex) {
            throw new ParallelExecutionException(ex.getCause());
        } catch (InterruptedException ex) {
            throw new ParallelExecutionException(ex.getCause());
        }
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
}
