package terrastore.communication.seda;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    public <R> Future<R> execute(ExecutionHandler<R> handler) {
        if (!shutdown) {
            Future<R> result = submit(handler);
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
}
