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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.advisory.DestinationSource;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import terrastore.event.Event;
import terrastore.event.EventBus;
import terrastore.event.EventListener;

/**
 * Durable and reliable {@link terrastore.event.EventBus} implementation based on the ActiveMQ message broker.
 * <br><br>
 * All events related to the same bucket are synchronously enqueued in the same JMS queue, named terrastore.<em>bucketName</em>,
 * and asynchronously processed by configured {@link terrastore.event.EventListener}s on a casual Terrastore node;
 * so, events enqueued on a given node may be concurrently processed on different nodes.<br>
 * However, {@link terrastore.event.Event}s referring to the same key in the same bucket are guaranteed to be sequentially processed in FIFO order,
 * to preserve per-document consistency.
 * <br><br>
 * Failing {@link terrastore.event.EventListener}s cause event redelivery: as a consequence, the same event may be processed several times by previously
 * successful listeners, and you should implement your listeners to be idempotent (for example by taking {@link terrastore.event.Event#getId()} into account).
 *
 * @author Sergio Bossa
 */
public class ActiveMQEventBus implements EventBus {

    private static final String TERRASTORE_QUEUE_PREFIX = "terrastore.";
    //
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQEventBus.class);
    //
    private final ConnectionFactory jmsConnectionFactory;
    private final JmsTemplate producer;
    private final ConcurrentMap<String, DefaultMessageListenerContainer> consumers = new ConcurrentHashMap<String, DefaultMessageListenerContainer>();
    private final Lock stateLock = new ReentrantLock();
    private final List<EventListener> eventListeners;
    private final boolean enabled;
    private volatile boolean shutdown;

    public ActiveMQEventBus(List<EventListener> eventListeners, String broker) throws Exception {
        LOG.info("Configuring event bus: {}", this.getClass().getName());
        this.eventListeners = eventListeners;
        this.jmsConnectionFactory = new PooledConnectionFactory(broker);
        this.producer = new JmsTemplate(jmsConnectionFactory);
        this.enabled = this.eventListeners.size() > 0;
        initListeners(this.eventListeners);
        initConsumers(broker);
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
                for (DefaultMessageListenerContainer consumer : consumers.values()) {
                    consumer.shutdown();
                }
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
            createConsumer(event);
            enqueue(event);
        } else if (shutdown) {
            throw new IllegalStateException("The bus has been shutdown!");
        }
    }

    private void initListeners(List<EventListener> eventListeners) {
        for (EventListener listener : eventListeners) {
            LOG.info("Configuring listener: {}", listener.getClass().getName());
            listener.init();
        }
    }

    private void initConsumers(String broker) throws Exception {
        ActiveMQConnection connection = null;
        DestinationSource destinations = null;
        try {
            connection = ActiveMQConnection.makeConnection(broker);
            connection.start();
            //
            destinations = connection.getDestinationSource();
            destinations.start();
            //
            Set<ActiveMQQueue> queues = destinations.getQueues();
            for (ActiveMQQueue queue : queues) {
                if (queue.getQueueName().startsWith(TERRASTORE_QUEUE_PREFIX)) {
                    LOG.info("Listening to queue: {}", queue.getQueueName());
                    DefaultMessageListenerContainer consumer = new DefaultMessageListenerContainer();
                    consumer.setConnectionFactory(jmsConnectionFactory);
                    consumer.setSessionTransacted(true);
                    consumer.setMessageListener(new EventProcessor(eventListeners));
                    consumer.setDestinationName(queue.getQueueName());
                    consumer.start();
                    consumer.initialize();
                    consumers.put(queue.getQueueName(), consumer);
                }
            }
        } finally {
            if (destinations != null) {
                destinations.stop();
            }
            if (connection != null) {
                connection.stop();
                connection.close();
            }
        }
    }

    private void createConsumer(Event event) {
        String queueName = TERRASTORE_QUEUE_PREFIX + event.getBucket();
        if (!consumers.containsKey(queueName)) {
            stateLock.lock();
            try {
                if (!shutdown && !consumers.containsKey(queueName)) {
                    DefaultMessageListenerContainer consumer = new DefaultMessageListenerContainer();
                    consumer.setConnectionFactory(jmsConnectionFactory);
                    consumer.setSessionTransacted(true);
                    consumer.setMessageListener(new EventProcessor(eventListeners));
                    consumer.setDestinationName(queueName);
                    consumer.start();
                    consumer.initialize();
                    consumers.put(queueName, consumer);
                }
            } finally {
                stateLock.unlock();
            }
        }
    }

    private void enqueue(final Event event) {
        String queueName = TERRASTORE_QUEUE_PREFIX + event.getBucket();
        producer.send(queueName, new MessageCreator() {

            @Override
            public Message createMessage(Session session) throws JMSException {
                Message message = session.createObjectMessage(event);
                message.setStringProperty("JMSXGroupID", new StringBuilder().append(event.getBucket()).append(":").append(event.getKey()).toString());
                return message;
            }
        });
    }

    private static class EventProcessor implements MessageListener {

        private final List<EventListener> eventListeners;

        public EventProcessor(List<EventListener> eventListeners) {
            this.eventListeners = new ArrayList<EventListener>(eventListeners);
        }

        @Override
        public void onMessage(Message message) {
            try {
                Event event = (Event) ((ObjectMessage) message).getObject();
                dispatch(event);
            } catch (JMSException ex) {
                // TODO: better handling?
                LOG.warn(ex.getMessage(), ex);
            }
        }

        private void dispatch(Event event) {
            for (EventListener listener : eventListeners) {
                if (listener.observes(event.getBucket())) {
                    event.dispatch(listener);
                }
            }
        }
    }
}
