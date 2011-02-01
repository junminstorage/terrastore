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
package terrastore.communication.process;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Sergio Bossa
 */
public class AsynchronousExecutor extends AbstractExecutor {

    private final ExecutorService executor;

    public AsynchronousExecutor() {
        this(Runtime.getRuntime().availableProcessors() * 10);
    }

    public AsynchronousExecutor(int threads) {
        executor = new ThreadPoolExecutor(threads, threads, 60, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
    }

    @Override
    protected void doExecute(Runnable task) {
        executor.submit(task);
    }

    @Override
    protected void doShutdown() {
        executor.shutdown();
    }
}
