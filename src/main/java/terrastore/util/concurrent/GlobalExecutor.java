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
package terrastore.util.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import jsr166y.ForkJoinPool;
import jsr166y.ForkJoinWorkerThread;

/**
 * @author Sergio Bossa
 */
public class GlobalExecutor {

    /**
     * TODO: work stealing would probably be much better, because not all executors will be equally used ...
     */

    private static volatile ExecutorService ACTION_EXECUTOR = newExecutor(Runtime.getRuntime().availableProcessors() * 2);
    private static volatile ExecutorService QUERY_EXECUTOR = newExecutor(Runtime.getRuntime().availableProcessors() * 2);
    private static volatile ExecutorService UPDATE_EXECUTOR = newExecutor(Runtime.getRuntime().availableProcessors() * 2);
    private static volatile ForkJoinPool FJ_POOL = newFJPool(Runtime.getRuntime().availableProcessors() * 2);

    public static void configure(int threads) {
        int minThreadsShare = Runtime.getRuntime().availableProcessors() * 2;
        int actual = threads / 3 > minThreadsShare ? threads / 3 : minThreadsShare;
        ACTION_EXECUTOR = newExecutor(actual);
        QUERY_EXECUTOR = newExecutor(actual);
        UPDATE_EXECUTOR = newExecutor(actual);
        FJ_POOL = newFJPool(minThreadsShare);
    }

    public static void shutdown() {
        ACTION_EXECUTOR.shutdownNow();
        QUERY_EXECUTOR.shutdownNow();
        UPDATE_EXECUTOR.shutdownNow();
        FJ_POOL.shutdownNow();
    }

    public static ExecutorService getActionExecutor() {
        return ACTION_EXECUTOR;
    }

    public static ExecutorService getQueryExecutor() {
        return QUERY_EXECUTOR;
    }

    public static ExecutorService getUpdateExecutor() {
        return UPDATE_EXECUTOR;
    }

    public static ForkJoinPool getForkJoinPool() {
        return FJ_POOL;
    }

    private static ExecutorService newExecutor(int threads) {
        return Executors.newFixedThreadPool(threads, new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }

        });
    }

    private static ForkJoinPool newFJPool(int parallelism) {
        return new ForkJoinPool(parallelism, new ForkJoinPool.ForkJoinWorkerThreadFactory() {

            @Override
            public ForkJoinWorkerThread newThread(ForkJoinPool fjp) {
                ForkJoinWorkerThread t = new ForkJoinWorkerThread(fjp) {
                };
                t.setDaemon(true);
                return t;
            }

        });
    }

}
