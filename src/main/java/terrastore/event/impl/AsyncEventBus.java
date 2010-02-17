package terrastore.event.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.event.Event;
import terrastore.event.EventBus;
import terrastore.event.EventListener;

/**
 * @author Sergio Bossa
 */
public class AsyncEventBus implements EventBus {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncEventBus.class);
    //
    private final Map<String, BlockingQueue<Event>> queues = new HashMap<String, BlockingQueue<Event>>();
    private final Map<String, EventProcessor> processors = new HashMap<String, EventProcessor>();
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final Lock stateLock = new ReentrantLock();
    private final List<EventListener> eventListeners;
    private final boolean enabled;

    public AsyncEventBus(List<EventListener> eventListeners) {
        this.eventListeners = eventListeners;
        this.enabled = this.eventListeners.size() > 0;
    }

    @Override
    public List<EventListener> getEventListeners() {
        return Collections.unmodifiableList(eventListeners);
    }

    @Override
    public void shutdown() {
        for (EventProcessor processor : processors.values()) {
            processor.stop();
        }
        threadPool.shutdownNow();
    }

    @Override
    public void publish(Event event) {
        if (enabled) {
            LOG.debug("Publishing event for bucket {} and value {}", event.getBucket(), event.getKey());
            boolean hasListeners = setupListeners(event);
            if (hasListeners) {
                stateLock.lock();
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
            processor = new EventProcessor(queue);
            queues.put(bucket, queue);
            processors.put(bucket, processor);
            threadPool.submit(processor);
        }
        queue.offer(event);
    }

    private static class EventProcessor implements Runnable {

        private final BlockingQueue<Event> queue;
        private volatile Thread currentThread;
        private volatile boolean running;

        public EventProcessor(BlockingQueue<Event> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            currentThread = Thread.currentThread();
            running = true;
            while (running) {
                try {
                    Event event = queue.take();
                    event.dispatch();
                } catch (InterruptedException ex) {
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
