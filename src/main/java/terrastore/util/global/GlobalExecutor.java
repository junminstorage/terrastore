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
package terrastore.util.global;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import jsr166y.ForkJoinPool;
import jsr166y.ForkJoinWorkerThread;

/**
 * @author Sergio Bossa
 */
public class GlobalExecutor {

    private static volatile ExecutorService EXECUTOR = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        }
    });
    private static volatile ForkJoinPool POOL = new ForkJoinPool(Runtime.getRuntime().availableProcessors(), new ForkJoinPool.ForkJoinWorkerThreadFactory() {

        @Override
        public ForkJoinWorkerThread newThread(ForkJoinPool fjp) {
            ForkJoinWorkerThread t = new ForkJoinWorkerThread(fjp) {};
            t.setDaemon(true);
            return t;
        }
    });

    public static void setExecutor(ExecutorService executor) {
        EXECUTOR = executor;
    }

    public static ExecutorService getExecutor() {
        return EXECUTOR;
    }

    public static void setForkJoinPool(ForkJoinPool pool) {
        POOL = pool;
    }

    public static ForkJoinPool getForkJoinPool() {
        return POOL;
    }
}
