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
package terrastore.communication.process;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Sergio Bossa
 */
public abstract class AbstractExecutor implements ThreadPool {

    private final static int ACTIVE = 1;
    private final static int PAUSED = 2;
    private final static int SHUTDOWN = 3;
    //
    private final Queue<Runnable> holdbackQueue;
    private final ExecutorService resumedTasksExecutor;
    private AtomicInteger status;
    private AtomicInteger executions;

    public AbstractExecutor() {
        holdbackQueue = new ConcurrentLinkedQueue<Runnable>();
        resumedTasksExecutor = Executors.newSingleThreadExecutor();
        status = new AtomicInteger(ACTIVE);
        executions = new AtomicInteger(0);
    }

    @Override
    public boolean isPaused() {
        return status.get() == PAUSED;
    }

    @Override
    public void pause() {
        if (status.compareAndSet(ACTIVE, PAUSED)) {
            while (executions.get() > 0) {
                backoff();
            }
        } else {
            throw new IllegalStateException("Thread pool is not in active state!");
        }
    }

    @Override
    public void resume() {
        if (status.compareAndSet(PAUSED, ACTIVE)) {
            resumedTasksExecutor.submit(new Runnable() {

                @Override
                public void run() {
                    Iterator<Runnable> holdbackQueueIt = holdbackQueue.iterator();
                    while (holdbackQueueIt.hasNext()) {
                        execute(Executors.callable(holdbackQueueIt.next()));
                        holdbackQueueIt.remove();
                    }
                }
            });
        } else {
            throw new IllegalStateException("Thread pool is not in paused state!");
        }
    }

    @Override
    public void shutdown() {
        status.set(SHUTDOWN);
        holdbackQueue.clear();
        resumedTasksExecutor.shutdown();
    }

    @Override
    public <R> Future<R> execute(Callable<R> callable) {
        while (true) {
            // Only execute if active:
            if (status.get() == ACTIVE) {
                // Increment to avoid pausing without first executing all tasks:
                executions.incrementAndGet();
                // Check again if active, to avoid race conditions with pause() method:
                if (status.get() == ACTIVE) {
                    // If still active, then execute:
                    return run(callable);
                } else {
                    // If paused, just decrement the previously incremented counter and retry until active:
                    executions.decrementAndGet();
                    backoff();
                    continue;
                }
            } else if (status.get() == PAUSED) {
                FutureTask<R> heldTask = new FutureTask<R>(callable);
                holdbackQueue.add(heldTask);
                return heldTask;
            } else if (status.get() == SHUTDOWN) {
                throw new IllegalStateException("Shutdown thread pool!");
            }
        }
    }

    /**
     * Implement actual execution logic.
     */
    protected abstract void doExecute(Runnable task);

    /**
     * Implement actual shutdown logic.
     */
    protected abstract void doShutdown();

    private <R> Future<R> run(Callable<R> task) {
        FutureTask<R> future = new FutureTask<R>(task) {

            @Override
            protected void done() {
                executions.decrementAndGet();
            }
        };
        doExecute(future);
        return future;
    }

    private void backoff() {
        // TODO: implement backoff strategy if needed ...
    }
}
