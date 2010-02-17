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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
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
 * All events related to the same bucket are sequentially processed in FIFO order, while events for unrelated buckets are concurrently processed.
 *
 * @author Sergio Bossa
 */
public class AsyncEventBus implements EventBus {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncEventBus.class);
    private static final int DEFAULT_MAX_IDLE_TIME = 60;
    //
    private final Map<String, BlockingQueue<Event>> queues = new HashMap<String, BlockingQueue<Event>>();
    private final Map<String, EventProcessor> processors = new HashMap<String, EventProcessor>();
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final Lock stateLock = new ReentrantLock();
    private final List<EventListener> eventListeners;
    private final int maxIdleTimeInSeconds;
    private final boolean enabled;

    public AsyncEventBus(List<EventListener> eventListeners) {
        this(eventListeners, DEFAULT_MAX_IDLE_TIME);
    }

    public AsyncEventBus(List<EventListener> eventListeners, int maxIdleTimeInSeconds) {
        this.eventListeners = eventListeners;
        this.maxIdleTimeInSeconds = maxIdleTimeInSeconds;
        this.enabled = this.eventListeners.size() > 0;
    }

    @Override
    public List<EventListener> getEventListeners() {
        return Collections.unmodifiableList(eventListeners);
    }

    @Override
    public void shutdown() {
        stateLock.lock();
        try {
            for (EventProcessor processor : processors.values()) {
                processor.stop();
            }
            threadPool.shutdownNow();
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void publish(Event event) {
        if (enabled) {
            LOG.debug("Publishing event for bucket {} and value {}", event.getBucket(), event.getKey());
            boolean hasListeners = setupListeners(event);
            if (hasListeners) {
                stateLock.lock();
                LOG.debug("Enqueuing event for bucket {} and value {}", event.getBucket(), event.getKey());
                try {
                    enqueue(event);
                } finally {
                    stateLock.unlock();
                }
            }
        }
    }

    private boolean setupListeners(Event event) {
        boolean found = false;
        for (EventListener listener : eventListeners) {
            if (listener.observes(event.getBucket())) {
                event.addEventListener(listener);
                found = true;
            }
        }
        return found;
    }

    private void enqueue(Event event) {
        String bucket = event.getBucket();
        BlockingQueue<Event> queue = queues.get(bucket);
        EventProcessor processor = null;
        if (queue == null) {
            queue = new LinkedBlockingQueue<Event>();
            processor = new EventProcessor(queue, new IdleCallback(bucket), maxIdleTimeInSeconds);
            queues.put(bucket, queue);
            processors.put(bucket, processor);
            threadPool.submit(processor);
        }
        queue.offer(event);
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

        private final BlockingQueue<Event> queue;
        private final IdleCallback idleCallback;
        private final int maxIdleTimeInSeconds;
        private volatile Thread currentThread;
        private volatile boolean running;

        public EventProcessor(BlockingQueue<Event> queue, IdleCallback idleCallback, int maxIdleTimeInSeconds) {
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
                        event.dispatch();
                    } else {
                        running = false;
                        idleCallback.execute();
                    }
                } catch (InterruptedException ex) {
                    waitTime = waitTime - (System.currentTimeMillis() - startTime);
                } catch (Exception ex) {
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
