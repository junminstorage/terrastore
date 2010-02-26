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
package terrastore.event.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.event.Event;
import terrastore.event.EventBus;
import terrastore.event.EventDispatcher;
import terrastore.event.EventListener;

/**
 * Asynchronous, memory-based, {@link terrastore.event.EventBus} implementation.<br>
 * All events related to the same bucket are sequentially processed in FIFO order, while events for unrelated buckets are concurrently processed.
 *
 * @author Sergio Bossa
 */
public class AsyncEventBus implements EventBus {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncEventBus.class);
    private static final int DEFAULT_MAX_IDLE_TIME = 60;
    //
    private final ConcurrentMap<String, BlockingQueue<EventDispatcher>> queues = new ConcurrentHashMap<String, BlockingQueue<EventDispatcher>>();
    private final ConcurrentMap<String, EventProcessor> processors = new ConcurrentHashMap<String, EventProcessor>();
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final Lock stateLock = new ReentrantLock();
    private final List<EventListener> eventListeners;
    private final int maxIdleTimeInSeconds;
    private final boolean enabled;
    private volatile boolean shutdown;

    public AsyncEventBus(List<EventListener> eventListeners) {
        this(eventListeners, DEFAULT_MAX_IDLE_TIME);
    }

    public AsyncEventBus(List<EventListener> eventListeners, int maxIdleTimeInSeconds) {
        this.eventListeners = eventListeners;
        this.maxIdleTimeInSeconds = maxIdleTimeInSeconds;
        this.enabled = this.eventListeners.size() > 0;
        initListeners(this.eventListeners);
    }

    @Override
    public List<EventListener> getEventListeners() {
        return Collections.unmodifiableList(eventListeners);
    }

    @Override
    public void shutdown() {
        if (!shutdown) {
            stateLock.lock();
            try {
                stopProcessors();
                cleanupListeners();
                threadPool.shutdownNow();
                shutdown = true;
            } finally {
                stateLock.unlock();
            }
        } else {
            throw new IllegalStateException("The bus has been shutdown!");
        }
    }

    @Override
    public void publish(Event event) {
        if (enabled && !shutdown) {
            LOG.debug("Publishing event for bucket {} and value {}", event.getBucket(), event.getKey());
            EventDispatcher dispatcher = setupEventDispatcher(event);
            if (dispatcher.hasListeners()) {
                enqueue(event, dispatcher);
            }
        } else if (shutdown) {
            throw new IllegalStateException("The bus has been shutdown!");
        }
    }

    private void initListeners(List<EventListener> eventListeners) {
        for (EventListener listener : eventListeners) {
            listener.init();
        }
    }

    private EventDispatcher setupEventDispatcher(Event event) {
        EventDispatcher dispatcher = new DefaultEventDispatcher(event);
        for (EventListener listener : eventListeners) {
            if (listener.observes(event.getBucket())) {
                dispatcher.addEventListener(listener);
            }
        }
        return dispatcher;
    }

    private void enqueue(Event event, EventDispatcher dispatcher) {
        LOG.debug("Enqueuing event for bucket {} and value {}", event.getBucket(), event.getKey());
        String bucket = event.getBucket();
        BlockingQueue<EventDispatcher> queue = queues.get(bucket);
        EventProcessor processor = null;
        if (queue == null) {
            stateLock.lock();
            try {
                if (!queues.containsKey(bucket)) {
                    queue = new LinkedBlockingQueue<EventDispatcher>();
                    processor = new EventProcessor(queue, new IdleCallback(bucket), maxIdleTimeInSeconds);
                    queues.put(bucket, queue);
                    processors.put(bucket, processor);
                    threadPool.submit(processor);
                }
            } finally {
                stateLock.unlock();
            }
        }
        queue.offer(dispatcher);
    }

    private void stopProcessors() {
        for (EventProcessor processor : processors.values()) {
            processor.stop();
        }
    }

    private void cleanupListeners() {
        for (EventListener listener : eventListeners) {
            listener.cleanup();
        }
    }

    private class IdleCallback {

        private final String bucket;

        public IdleCallback(String bucket) {
            this.bucket = bucket;
        }

        public void execute() {
            stateLock.lock();
            try {
                queues.remove(bucket);
                processors.remove(bucket);
            } finally {
                stateLock.unlock();
            }
        }
    }

    private static class EventProcessor implements Runnable {

        private final BlockingQueue<EventDispatcher> queue;
        private final IdleCallback idleCallback;
        private final int maxIdleTimeInSeconds;
        private volatile Thread currentThread;
        private volatile boolean running;

        public EventProcessor(BlockingQueue<EventDispatcher> queue, IdleCallback idleCallback, int maxIdleTimeInSeconds) {
            this.queue = queue;
            this.idleCallback = idleCallback;
            this.maxIdleTimeInSeconds = maxIdleTimeInSeconds;
        }

        @Override
        public void run() {
            long waitTime = TimeUnit.MILLISECONDS.convert(maxIdleTimeInSeconds, TimeUnit.SECONDS);
            long startTime = 0;
            currentThread = Thread.currentThread();
            running = true;
            while (running && waitTime > 0) {
                startTime = System.currentTimeMillis();
                try {
                    EventDispatcher eventDispatcher = queue.poll(waitTime, TimeUnit.MILLISECONDS);
                    if (eventDispatcher != null) {
                        waitTime = TimeUnit.MILLISECONDS.convert(maxIdleTimeInSeconds, TimeUnit.SECONDS);
                        eventDispatcher.dispatch();
                    } else {
                        running = false;
                        idleCallback.execute();
                    }
                } catch (InterruptedException ex) {
                    waitTime = waitTime - (System.currentTimeMillis() - startTime);
                } catch (Exception ex) {
                    // TODO: improve error handling!
                    LOG.warn(ex.getMessage(), ex);
                }
            }
            Thread.interrupted();
        }

        public void stop() {
            running = false;
            currentThread.interrupt();
        }
    }
}
