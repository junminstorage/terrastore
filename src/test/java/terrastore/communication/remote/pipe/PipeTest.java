/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.communication.remote.pipe;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import terrastore.communication.serialization.Serializer;
import terrastore.store.Value;
import terrastore.test.support.TesterThreadFactory;
import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

public class PipeTest {

    private final String MESSAGE = "message";
    private final Value VALUE = new Value(MESSAGE.getBytes());
    private final TesterThreadFactory threadFactory = new TesterThreadFactory();

    @Test
    public void testPutAndPeek() throws Exception {
        Serializer<Value> serializer = createMock(Serializer.class);
        serializer.serialize(VALUE);
        expectLastCall().andReturn(MESSAGE).once();
        serializer.deserialize(MESSAGE);
        expectLastCall().andReturn(VALUE).once();

        replay(serializer);

        Pipe<Value> pipe = new Pipe<Value>(serializer);
        pipe.put(VALUE);
        assertEquals(1, pipe.size());
        assertEquals(VALUE, pipe.peek());

        verify(serializer);
    }

    @Test
    public void testPutAndTake() throws Exception {
        Serializer<Value> serializer = createMock(Serializer.class);
        serializer.serialize(VALUE);
        expectLastCall().andReturn(MESSAGE).once();
        serializer.deserialize(MESSAGE);
        expectLastCall().andReturn(VALUE).once();

        replay(serializer);

        Pipe<Value> pipe = new Pipe<Value>(serializer);
        pipe.put(VALUE);
        assertEquals(1, pipe.size());
        assertEquals(VALUE, pipe.take());

        verify(serializer);
    }

    @Test
    public void testPutAndPoll() throws Exception {
        Serializer<Value> serializer = createMock(Serializer.class);
        serializer.serialize(VALUE);
        expectLastCall().andReturn(MESSAGE).once();
        serializer.deserialize(MESSAGE);
        expectLastCall().andReturn(VALUE).once();

        replay(serializer);

        Pipe<Value> pipe = new Pipe<Value>(serializer);
        pipe.put(VALUE);
        assertEquals(1, pipe.size());
        assertEquals(VALUE, pipe.poll());

        verify(serializer);
    }

    @Test
    public void testOfferAndPeek() throws Exception {
        Serializer<Value> serializer = createMock(Serializer.class);
        serializer.serialize(VALUE);
        expectLastCall().andReturn(MESSAGE).once();
        serializer.deserialize(MESSAGE);
        expectLastCall().andReturn(VALUE).once();

        replay(serializer);

        Pipe<Value> pipe = new Pipe<Value>(serializer);
        pipe.offer(VALUE);
        assertEquals(1, pipe.size());
        assertEquals(VALUE, pipe.peek());

        verify(serializer);
    }

    @Test
    public void testOfferAndTake() throws Exception {
        Serializer<Value> serializer = createMock(Serializer.class);
        serializer.serialize(VALUE);
        expectLastCall().andReturn(MESSAGE).once();
        serializer.deserialize(MESSAGE);
        expectLastCall().andReturn(VALUE).once();

        replay(serializer);

        Pipe<Value> pipe = new Pipe<Value>(serializer);
        pipe.offer(VALUE);
        assertEquals(1, pipe.size());
        assertEquals(VALUE, pipe.take());

        verify(serializer);
    }

    @Test
    public void testOfferAndPoll() throws Exception {
        Serializer<Value> serializer = createMock(Serializer.class);
        serializer.serialize(VALUE);
        expectLastCall().andReturn(MESSAGE).once();
        serializer.deserialize(MESSAGE);
        expectLastCall().andReturn(VALUE).once();

        replay(serializer);

        Pipe<Value> pipe = new Pipe<Value>(serializer);
        pipe.offer(VALUE);
        assertEquals(1, pipe.size());
        assertEquals(VALUE, pipe.poll());

        verify(serializer);
    }

    @Test
    public void testPollGoesInTimeout() throws Exception {
        Serializer<Value> serializer = createMock(Serializer.class);

        replay(serializer);

        Pipe<Value> pipe = new Pipe<Value>(serializer);
        Value value = pipe.poll(1, TimeUnit.SECONDS);
        assertNull(value);
        assertEquals(0, pipe.size());

        verify(serializer);
    }

    @Test
    public void testPutAndPollFromDifferentThreadsWithTimeout() throws Exception {
        Serializer<Value> serializer = createMock(Serializer.class);
        makeThreadSafe(serializer, true);
        serializer.serialize(VALUE);
        expectLastCall().andReturn(MESSAGE).once();
        serializer.deserialize(MESSAGE);
        expectLastCall().andReturn(VALUE).once();

        replay(serializer);

        final Pipe<Value> pipe = new Pipe<Value>(serializer);

        final CountDownLatch stopLatch = new CountDownLatch(2);

        Thread pollThread = threadFactory.newThread(new Runnable() {

            public void run() {
                try {
                    Value polled = pipe.poll(60, TimeUnit.SECONDS);
                    assertEquals(VALUE, polled);
                } catch (InterruptedException ex) {
                    fail(ex.getMessage());
                } finally {
                    stopLatch.countDown();
                }
            }
        });
        Thread putThread = threadFactory.newThread(new Runnable() {

            public void run() {
                try {
                    pipe.put(VALUE);
                } catch (InterruptedException ex) {
                    fail(ex.getMessage());
                } finally {
                    stopLatch.countDown();
                }
            }
        });

        pollThread.start();
        putThread.start();

        stopLatch.await();

        assertEquals(0, pipe.size());

        threadFactory.verifyThreads();
        threadFactory.resetThreads();

        verify(serializer);
    }
}