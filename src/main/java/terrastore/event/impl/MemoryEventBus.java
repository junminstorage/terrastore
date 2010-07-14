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
import terrastore.event.EventListener;

/**
 * Asynchronous, memory-based, {@link terrastore.event.EventBus} implementation.<br>
 * All events related to the same bucket are sequentially processed in FIFO order by configured {@link terrastore.event.EventListener}s,
 * while events for unrelated buckets are concurrently processed.<br>
 * {@link terrastore.event.EventListener} execution is <strong>lenient</strong>, meaning that failing listeners will be ignored and will not stop
 * execution of subsequent listeners, so program your listeners accordingly.
 *
 * @author Sergio Bossa
 */
public class MemoryEventBus implements EventBus {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryEventBus.class);
    private static final int DEFAULT_MAX_IDLE_TIME = 60;
    //
    private final ConcurrentMap<String, BlockingQueue<Event>> queues = new ConcurrentHashMap<String, BlockingQueue<Event>>();
    private final ConcurrentMap<String, EventProcessor> processors = new ConcurrentHashMap<String, EventProcessor>();
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final Lock stateLock = new ReentrantLock();
    private final List<EventListener> eventListeners;
    private final int maxIdleTimeInSeconds;
    private final boolean enabled;
    private volatile boolean shutdown;

    public MemoryEventBus(List<EventListener> eventListeners) {
        this(eventListeners, DEFAULT_MAX_IDLE_TIME);
    }

    public MemoryEventBus(List<EventListener> eventListeners, int maxIdleTimeInSeconds) {
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
            enqueue(event);
        } else if (shutdown) {
            throw new IllegalStateException("The bus has been shutdown!");
        }
    }

    private void initListeners(List<EventListener> eventListeners) {
        for (EventListener listener : eventListeners) {
            listener.init();
        }
    }

    private void enqueue(Event event) {
        LOG.debug("Enqueuing event for bucket {} and value {}", event.getBucket(), event.getKey());
        String bucket = event.getBucket();
        BlockingQueue<Event> queue = queues.get(bucket);
        EventProcessor processor = null;
        if (queue == null) {
            stateLock.lock();
            try {
                if (!queues.containsKey(bucket)) {
                    queue = new LinkedBlockingQueue<Event>();
                    processor = new EventProcessor(eventListeners, queue, new IdleCallback(bucket), maxIdleTimeInSeconds);
                    queues.put(bucket, queue);
                    processors.put(bucket, processor);
                    threadPool.submit(processor);
                }
            } finally {
                stateLock.unlock();
            }
        }
        queue.offer(event);
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

        private final List<EventListener> eventListeners;
        private final BlockingQueue<Event> queue;
        private final IdleCallback idleCallback;
        private final int maxIdleTimeInSeconds;
        private volatile Thread currentThread;
        private volatile boolean running;

        public EventProcessor(List<EventListener> eventListeners, BlockingQueue<Event> queue, IdleCallback idleCallback, int maxIdleTimeInSeconds) {
            this.eventListeners = eventListeners;
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
                    Event event = queue.poll(waitTime, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        waitTime = TimeUnit.MILLISECONDS.convert(maxIdleTimeInSeconds, TimeUnit.SECONDS);
                        dispatch(event);
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

        private void dispatch(Event event) {
            for (EventListener listener : eventListeners) {
                if (listener.observes(event.getBucket())) {
                    try {
                        event.dispatch(listener);
                    } catch (Exception ex) {
                        LOG.warn("Failed listener: " + listener.toString());
                        LOG.warn(ex.getMessage(), ex);
                    }
                }
            }
        }
    }
}
