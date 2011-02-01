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
package terrastore.event.impl;

import terrastore.store.Value;
import terrastore.event.Event;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;
import terrastore.event.ActionExecutor;
import terrastore.event.EventListener;
import static org.easymock.classextension.EasyMock.*;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class MemoryEventBusTest {

    @Test
    public void testValueChangedEvent() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(1);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");
        Event event = new ValueChangedEvent(bucket, key, null, new Value(value));

        ActionExecutor actionExecutor = createMock(ActionExecutor.class);
        makeThreadSafe(actionExecutor, true);
        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.observes(bucket);
        expectLastCall().andReturn(true);
        listener.onValueChanged(eq(event), same(actionExecutor));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();
        listener.init();
        expectLastCall().once();
        listener.cleanup();
        expectLastCall().once();

        replay(listener);

        MemoryEventBus eventBus = new MemoryEventBus(Arrays.asList(listener), actionExecutor);

        eventBus.publish(event);

        listenerLatch.await(3, TimeUnit.SECONDS);

        eventBus.shutdown();

        verify(listener);
    }

    @Test
    public void testValueRemovedEvent() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(1);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");
        Event event = new ValueRemovedEvent(bucket, key, new Value(value));

        ActionExecutor actionExecutor = createMock(ActionExecutor.class);
        makeThreadSafe(actionExecutor, true);
        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.observes(bucket);
        expectLastCall().andReturn(true);
        listener.onValueRemoved(eq(event), same(actionExecutor));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();
        listener.init();
        expectLastCall().once();
        listener.cleanup();
        expectLastCall().once();

        replay(listener);

        MemoryEventBus eventBus = new MemoryEventBus(Arrays.asList(listener), actionExecutor);

        eventBus.publish(event);

        listenerLatch.await(3, TimeUnit.SECONDS);

        eventBus.shutdown();

        verify(listener);
    }

    @Test
    public void testPublishEventWithAllListenersObserving() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(2);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");
        Event event = new ValueChangedEvent(bucket, key, null, new Value(value));

        ActionExecutor actionExecutor = createMock(ActionExecutor.class);
        makeThreadSafe(actionExecutor, true);
        EventListener listener1 = createMock(EventListener.class);
        EventListener listener2 = createMock(EventListener.class);
        makeThreadSafe(listener1, true);
        makeThreadSafe(listener2, true);
        listener1.observes(bucket);
        expectLastCall().andReturn(true);
        listener2.observes(bucket);
        expectLastCall().andReturn(true);
        listener1.onValueChanged(eq(event), same(actionExecutor));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();
        listener2.onValueChanged(eq(event), same(actionExecutor));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();
        listener1.init();
        expectLastCall().once();
        listener2.init();
        expectLastCall().once();
        listener1.cleanup();
        expectLastCall().once();
        listener2.cleanup();
        expectLastCall().once();

        replay(listener1, listener2);

        MemoryEventBus eventBus = new MemoryEventBus(Arrays.asList(listener1, listener2), actionExecutor);

        eventBus.publish(event);

        listenerLatch.await(3, TimeUnit.SECONDS);

        eventBus.shutdown();

        verify(listener1, listener2);
    }

    @Test
    public void testPublishEventWithOnlyOneListenerObserving() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(1);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");
        Event event = new ValueChangedEvent(bucket, key, null, new Value(value));

        ActionExecutor actionExecutor = createMock(ActionExecutor.class);
        makeThreadSafe(actionExecutor, true);
        EventListener listener1 = createMock(EventListener.class);
        EventListener listener2 = createMock(EventListener.class);
        makeThreadSafe(listener1, true);
        makeThreadSafe(listener2, true);
        listener1.observes(bucket);
        expectLastCall().andReturn(true);
        listener2.observes(bucket);
        expectLastCall().andReturn(false);
        listener1.onValueChanged(eq(event), same(actionExecutor));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();
        listener1.init();
        expectLastCall().once();
        listener2.init();
        expectLastCall().once();
        listener1.cleanup();
        expectLastCall().once();
        listener2.cleanup();
        expectLastCall().once();

        replay(listener1, listener2);

        MemoryEventBus eventBus = new MemoryEventBus(Arrays.asList(listener1, listener2), actionExecutor);

        eventBus.publish(event);

        listenerLatch.await(3, TimeUnit.SECONDS);

        eventBus.shutdown();

        verify(listener1, listener2);
    }

    @Test
    public void testPublishMoreEvents() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(4);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");
        Event event1 = new ValueChangedEvent(bucket, key, null, new Value(value));
        Event event2 = new ValueChangedEvent(bucket, key, null, new Value(value));

        ActionExecutor actionExecutor = createMock(ActionExecutor.class);
        makeThreadSafe(actionExecutor, true);
        EventListener listener1 = createMock(EventListener.class);
        EventListener listener2 = createMock(EventListener.class);
        makeThreadSafe(listener1, true);
        makeThreadSafe(listener2, true);
        listener1.observes(bucket);
        expectLastCall().andReturn(true).times(2);
        listener2.observes(bucket);
        expectLastCall().andReturn(true).times(2);
        listener1.onValueChanged(eq(event1), same(actionExecutor));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        });
        listener1.onValueChanged(eq(event2), same(actionExecutor));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        });
        listener2.onValueChanged(eq(event1), same(actionExecutor));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        });
        listener2.onValueChanged(eq(event2), same(actionExecutor));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        });
        listener1.init();
        expectLastCall().once();
        listener2.init();
        expectLastCall().once();
        listener1.cleanup();
        expectLastCall().once();
        listener2.cleanup();
        expectLastCall().once();

        replay(listener1, listener2);

        MemoryEventBus eventBus = new MemoryEventBus(Arrays.asList(listener1, listener2), actionExecutor);

        eventBus.publish(event1);
        eventBus.publish(event2);

        listenerLatch.await(3, TimeUnit.SECONDS);

        eventBus.shutdown();

        verify(listener1, listener2);
    }

    @Test
    public void testMultithreadedPublishing() throws Exception {
        int threads = 100;

        final CountDownLatch publishingLatch = new CountDownLatch(threads);
        final String bucket = "bucket";
        final String key = "key";
        final byte[] value = "value".getBytes("UTF-8");

        ActionExecutor actionExecutor = createMock(ActionExecutor.class);
        makeThreadSafe(actionExecutor, true);
        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.observes(bucket);
        expectLastCall().andReturn(true).times(threads);
        listener.onValueChanged(EasyMock.<Event>anyObject(), same(actionExecutor));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                publishingLatch.countDown();
                return null;
            }
        }).times(threads);
        listener.init();
        expectLastCall().once();
        listener.cleanup();
        expectLastCall().once();

        replay(listener);

        final MemoryEventBus eventBus = new MemoryEventBus(Arrays.asList(listener), actionExecutor);
        final ExecutorService publisher = Executors.newCachedThreadPool();
        for (int i = 0; i < threads; i++) {
            publisher.submit(new Runnable() {

                @Override
                public void run() {
                    eventBus.publish(new ValueChangedEvent(bucket, key, null, new Value(value)));
                }
            });
        }

        assertTrue(publishingLatch.await(threads, TimeUnit.SECONDS));

        // Sleep to avoid bad timing between listener answer and mock verification
        Thread.sleep(1000);
        //

        eventBus.shutdown();

        verify(listener);
    }

    @Test
    public void testPublishWaitForIdleTimeoutAndPublishAgain() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(2);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");

        ActionExecutor actionExecutor = createMock(ActionExecutor.class);
        makeThreadSafe(actionExecutor, true);
        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.observes(bucket);
        expectLastCall().andReturn(true).times(2);
        listener.onValueChanged(EasyMock.<Event>anyObject(), same(actionExecutor));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).times(2);
        listener.init();
        expectLastCall().once();
        listener.cleanup();
        expectLastCall().once();

        replay(listener);

        MemoryEventBus eventBus = new MemoryEventBus(Arrays.asList(listener), actionExecutor, 1);

        eventBus.publish(new ValueChangedEvent(bucket, key, null, new Value(value)));
        Thread.sleep(3000);
        eventBus.publish(new ValueChangedEvent(bucket, key, null, new Value(value)));

        listenerLatch.await(3, TimeUnit.SECONDS);

        eventBus.shutdown();

        verify(listener);
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotPublishAfterShutdown() throws Exception {
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");

        ActionExecutor actionExecutor = createMock(ActionExecutor.class);
        makeThreadSafe(actionExecutor, true);
        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.init();
        expectLastCall().once();
        listener.cleanup();
        expectLastCall().once();

        replay(listener);
        try {
            MemoryEventBus eventBus = new MemoryEventBus(Arrays.asList(listener), actionExecutor);
            eventBus.shutdown();
            eventBus.publish(new ValueChangedEvent(bucket, key, null, new Value(value)));
        } finally {
            verify(listener);
        }
    }

    @Test
    public void testLenientBehaviorOnException() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(2);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");

        ActionExecutor actionExecutor = createMock(ActionExecutor.class);
        makeThreadSafe(actionExecutor, true);
        EventListener listener1 = createMock(EventListener.class);
        EventListener listener2 = createMock(EventListener.class);
        makeThreadSafe(listener1, true);
        makeThreadSafe(listener2, true);
        listener1.observes(bucket);
        expectLastCall().andReturn(true);
        listener2.observes(bucket);
        expectLastCall().andReturn(true);
        listener1.onValueChanged(EasyMock.<Event>anyObject(), same(actionExecutor));
        expectLastCall().andThrow(new RuntimeException()).once();
        listener2.onValueChanged(EasyMock.<Event>anyObject(), same(actionExecutor));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();
        listener1.init();
        expectLastCall().once();
        listener2.init();
        expectLastCall().once();
        listener1.cleanup();
        expectLastCall().once();
        listener2.cleanup();
        expectLastCall().once();

        replay(listener1, listener2);

        MemoryEventBus eventBus = new MemoryEventBus(Arrays.asList(listener1, listener2), actionExecutor);

        eventBus.publish(new ValueChangedEvent(bucket, key, null, new Value(value)));

        assertFalse(listenerLatch.await(3, TimeUnit.SECONDS));

        eventBus.shutdown();

        verify(listener1, listener2);
    }
}
