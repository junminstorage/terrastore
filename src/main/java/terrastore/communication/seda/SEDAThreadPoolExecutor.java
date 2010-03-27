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
package terrastore.communication.seda;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import terrastore.communication.protocol.Command;

/**
 * @author Sergio Bossa
 */
public class SEDAThreadPoolExecutor extends ThreadPoolExecutor implements SEDAThreadPool {

    private final Lock stateLock;
    private final Condition pauseCondition;
    private final Condition inactiveCondition;
    private int activeThreads;
    private volatile boolean paused;
    private volatile boolean shutdown;

    public SEDAThreadPoolExecutor() {
        this(Runtime.getRuntime().availableProcessors() * 10);
    }

    public SEDAThreadPoolExecutor(int threads) {
        super(Runtime.getRuntime().availableProcessors(), threads, 60, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
        stateLock = new ReentrantLock();
        pauseCondition = stateLock.newCondition();
        inactiveCondition = stateLock.newCondition();
        activeThreads = 0;
        paused = false;
        shutdown = false;
    }

    @Override
    public boolean isPaused() {
        if (!shutdown) {
            return paused;
        } else {
            return false;
        }
    }

    public void pause() {
        stateLock.lock();
        try {
            if (!shutdown && !paused) {
                while (activeThreads > 0) {
                    try {
                        inactiveCondition.await();
                    } catch (InterruptedException ex) {
                    }
                }
                paused = true;
            } else if (shutdown) {
                throw new IllegalStateException("Shutdown SEDA thread pool!");
            }
        } finally {
            stateLock.unlock();
        }
    }

    public void resume() {
        stateLock.lock();
        try {
            if (!shutdown && paused) {
                paused = false;
                pauseCondition.signalAll();
            } else if (shutdown) {
                throw new IllegalStateException("Shutdown SEDA thread pool!");
            }
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        stateLock.lock();
        try {
            shutdown = true;
            super.shutdown();
        } finally {
            stateLock.unlock();
        }
    }

    public <R> Future<R> execute(Command<R> command, CommandHandler<R> handler) {
        if (!shutdown) {
            Future<R> result = submit(new CommandCallable<R>(command, handler));
            return result;
        } else {
            throw new IllegalStateException("Shutdown SEDA thread pool!");
        }
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        stateLock.lock();
        try {
            while (paused) {
                try {
                    pauseCondition.await();
                } catch (InterruptedException ex) {
                }
            }
            activeThreads++;
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        stateLock.lock();
        try {
            activeThreads--;
            if (activeThreads == 0) {
                inactiveCondition.signalAll();
            }
        } finally {
            stateLock.unlock();
        }
    }

    private static class CommandCallable<R> implements Callable<R> {

        private final Command<R> command;
        private final CommandHandler<R> handler;

        public CommandCallable(Command<R> command, CommandHandler<R> handler) {
            this.command = command;
            this.handler = handler;
        }

        @Override
        public R call() throws Exception {
            return handler.handle(command);
        }
    }
}
