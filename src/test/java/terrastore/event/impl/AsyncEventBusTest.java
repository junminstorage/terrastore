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

import terrastore.event.ValueChangedEvent;
import terrastore.event.ValueRemovedEvent;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.easymock.IAnswer;
import org.junit.Test;
import terrastore.event.EventListener;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class AsyncEventBusTest {

    @Test
    public void testValueChangedEvent() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(1);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");

        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.observes(bucket);
        expectLastCall().andReturn(true);
        listener.onValueChanged(eq(key), aryEq(value));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();

        replay(listener);

        AsyncEventBus eventBus = new AsyncEventBus(Arrays.asList(listener));

        eventBus.publish(new ValueChangedEvent(bucket, key, value));

        listenerLatch.await(3, TimeUnit.SECONDS);

        verify(listener);

        eventBus.shutdown();
    }

    @Test
    public void testValueRemovedEvent() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(1);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");

        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.observes(bucket);
        expectLastCall().andReturn(true);
        listener.onValueRemoved(eq(key));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();

        replay(listener);

        AsyncEventBus eventBus = new AsyncEventBus(Arrays.asList(listener));

        eventBus.publish(new ValueRemovedEvent(bucket, key));

        listenerLatch.await(3, TimeUnit.SECONDS);

        verify(listener);

        eventBus.shutdown();
    }

    @Test
    public void testPublishEventWithAllListenersObserving() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(2);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");

        EventListener listener1 = createMock(EventListener.class);
        EventListener listener2 = createMock(EventListener.class);
        makeThreadSafe(listener1, true);
        makeThreadSafe(listener2, true);
        listener1.observes(bucket);
        expectLastCall().andReturn(true);
        listener2.observes(bucket);
        expectLastCall().andReturn(true);
        listener1.onValueChanged(eq(key), aryEq(value));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();
        listener2.onValueChanged(eq(key), aryEq(value));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();

        replay(listener1, listener2);

        AsyncEventBus eventBus = new AsyncEventBus(Arrays.asList(listener1, listener2));

        eventBus.publish(new ValueChangedEvent(bucket, key, value));

        listenerLatch.await(3, TimeUnit.SECONDS);

        verify(listener1, listener2);

        eventBus.shutdown();
    }

    @Test
    public void testPublishEventWithOnlyOneListenerObserving() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(1);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");

        EventListener listener1 = createMock(EventListener.class);
        EventListener listener2 = createMock(EventListener.class);
        makeThreadSafe(listener1, true);
        makeThreadSafe(listener2, true);
        listener1.observes(bucket);
        expectLastCall().andReturn(true);
        listener2.observes(bucket);
        expectLastCall().andReturn(false);
        listener1.onValueChanged(eq(key), aryEq(value));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();

        replay(listener1, listener2);

        AsyncEventBus eventBus = new AsyncEventBus(Arrays.asList(listener1, listener2));

        eventBus.publish(new ValueChangedEvent(bucket, key, value));

        listenerLatch.await(3, TimeUnit.SECONDS);

        verify(listener1, listener2);

        eventBus.shutdown();
    }

    @Test
    public void testPublishMoreEvents() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(4);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");

        EventListener listener1 = createMock(EventListener.class);
        EventListener listener2 = createMock(EventListener.class);
        makeThreadSafe(listener1, true);
        makeThreadSafe(listener2, true);
        listener1.observes(bucket);
        expectLastCall().andReturn(true).times(2);
        listener2.observes(bucket);
        expectLastCall().andReturn(true).times(2);
        listener1.onValueChanged(eq(key), aryEq(value));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).times(2);
        listener2.onValueChanged(eq(key), aryEq(value));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).times(2);

        replay(listener1, listener2);

        AsyncEventBus eventBus = new AsyncEventBus(Arrays.asList(listener1, listener2));

        eventBus.publish(new ValueChangedEvent(bucket, key, value));
        eventBus.publish(new ValueChangedEvent(bucket, key, value));

        listenerLatch.await(3, TimeUnit.SECONDS);

        verify(listener1, listener2);

        eventBus.shutdown();
    }

    @Test
    public void testMultithreadedPublishing() throws Exception {
        int threads = 100;

        final CountDownLatch publishingLatch = new CountDownLatch(threads);
        final String bucket = "bucket";
        final String key = "key";
        final byte[] value = "value".getBytes("UTF-8");

        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.observes(bucket);
        expectLastCall().andReturn(true).times(threads);
        listener.onValueChanged(eq(key), aryEq(value));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                publishingLatch.countDown();
                return null;
            }
        }).times(threads);

        replay(listener);

        final AsyncEventBus eventBus = new AsyncEventBus(Arrays.asList(listener));
        final ExecutorService publisher = Executors.newCachedThreadPool();
        for (int i = 0; i < threads; i++) {
            publisher.submit(new Runnable() {

                @Override
                public void run() {
                    eventBus.publish(new ValueChangedEvent(bucket, key, value));
                }
            });
        }

        publishingLatch.await(threads, TimeUnit.SECONDS);

        verify(listener);

        eventBus.shutdown();
    }

    @Test
    public void testPublishWaitForIdleTimeoutAndPublishAgain() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(2);
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");

        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.observes(bucket);
        expectLastCall().andReturn(true).times(2);
        listener.onValueChanged(eq(key), aryEq(value));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).times(2);

        replay(listener);

        AsyncEventBus eventBus = new AsyncEventBus(Arrays.asList(listener), 1);

        eventBus.publish(new ValueChangedEvent(bucket, key, value));
        Thread.sleep(3000);
        eventBus.publish(new ValueChangedEvent(bucket, key, value));

        listenerLatch.await(3, TimeUnit.SECONDS);

        verify(listener);

        eventBus.shutdown();
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotPublishAfterShutdown() throws Exception {
        String bucket = "bucket";
        String key = "key";
        byte[] value = "value".getBytes("UTF-8");

        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.observes(bucket);
        expectLastCall().andReturn(true).once();

        replay(listener);
        try {
            AsyncEventBus eventBus = new AsyncEventBus(Arrays.asList(listener));
            eventBus.shutdown();
            eventBus.publish(new ValueChangedEvent(bucket, key, value));
        } finally {
            verify(listener);
        }
    }
}
