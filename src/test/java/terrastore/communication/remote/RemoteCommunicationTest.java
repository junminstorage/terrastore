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
package terrastore.communication.remote;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.easymock.classextension.EasyMock;
import org.junit.Test;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.GetValueCommand;
import terrastore.router.Router;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.features.Update;
import terrastore.store.operators.Condition;
import terrastore.store.operators.Function;
import static org.easymock.classextension.EasyMock.*;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class RemoteCommunicationTest {

    private static final String VALUE = "test";

    @Test
    public void testSendProcessAndReceive() throws Exception {
        String nodeName = "node";
        String bucketName = "bucket";
        String valueKey = "key";
        Value value = new TestValue(VALUE);
        Map<String, Value> values = new HashMap<String, Value>();
        values.put(valueKey, value);

        Router router = createMock(Router.class);
        Node node = createMock(Node.class);

        router.routeToNodeFor(bucketName, valueKey);
        expectLastCall().andReturn(node).once();
        node.send(EasyMock.<GetValueCommand>anyObject());
        expectLastCall().andReturn(value).once();

        replay(router, node);

        RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9990, 3145728, 10, router);
        Node sender = new RemoteNode("127.0.0.1", 9990, nodeName, 3145728, 1000);
        GetValueCommand command = new GetValueCommand(bucketName, valueKey);

        try {
            processor.start();
            sender.connect();

            Value result = sender.<Value>send(command);
            assertNotNull(result);
            assertEquals(new String(value.getBytes()), new String(result.getBytes()));
        } finally {
            try {
                sender.disconnect();
                processor.stop();
            } finally {
                verify(router, node);
            }
        }
    }

    @Test
    public void testMultithreadSendProcessAndReceive() throws Exception {
        final String nodeName = "node";
        final String bucketName = "bucket";
        final String valueKey = "key";
        final Value value = new TestValue(VALUE);
        final Map<String, Value> values = new HashMap<String, Value>();
        values.put(valueKey, value);

        final Router router = createMock(Router.class);
        final Node node = createMock(Node.class);
        makeThreadSafe(router, true);
        makeThreadSafe(node, true);

        router.routeToNodeFor(bucketName, valueKey);
        expectLastCall().andReturn(node).anyTimes();
        node.send(EasyMock.<GetValueCommand>anyObject());
        expectLastCall().andReturn(value).anyTimes();

        replay(router, node);

        final RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9990, 3145728, 10, router);
        processor.start();
        try {
            final AtomicBoolean corrupted = new AtomicBoolean(false);
            final AtomicBoolean failed = new AtomicBoolean(false);
            final ExecutorService executor = Executors.newCachedThreadPool();
            final int threads = 100;
            for (int i = 0; i < threads && corrupted.get() == false && failed.get() == false; i++) {
                executor.submit(new Runnable() {

                    @Override
                    public void run() {
                        Node sender = null;
                        try {
                            sender = new RemoteNode("127.0.0.1", 9990, nodeName, 3145728, 3000);
                            sender.connect();
                            //
                            GetValueCommand command = new GetValueCommand(bucketName, valueKey);
                            Value result = sender.<Value>send(command);
                            if (result == null || !Arrays.equals(value.getBytes(), result.getBytes())) {
                                corrupted.set(true);
                            }
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            failed.set(true);
                        } finally {
                            sender.disconnect();
                        }
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            if (corrupted.get()) {
                fail("Corrupted data!");
            }
            if (failed.get()) {
                fail("Failed!");
            }
        } finally {
            try {
                processor.stop();
            } finally {
                verify(router, node);
            }
        }
    }

    @Test(expected = ProcessingException.class)
    public void testCommunicationTimeout() throws Exception {
        String nodeName = "node";
        String bucketName = "bucket";
        String valueKey = "key";
        Value value = new TestValue(VALUE);
        Map<String, Value> values = new HashMap<String, Value>();
        values.put(valueKey, value);

        Router router = createMock(Router.class);
        Node node = createMock(Node.class);

        replay(router, node);

        RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9991, 3145728, 10, router);
        Node sender = new RemoteNode("127.0.0.1", 9991, nodeName, 3145728, 1000);
        GetValueCommand command = new GetValueCommand(bucketName, valueKey);

        try {
            // Start processor
            processor.start();
            // Connect node:
            sender.connect();
            // Stop processor so that no communication can happen:
            processor.stop();
            // Try to send:
            sender.<Value>send(command);
        } finally {
            verify(router, node);
        }
    }

    private static class TestValue implements Value {

        private final String content;

        public TestValue(String content) {
            this.content = content;
        }

        @Override
        public byte[] getBytes() {
            try {
                return content.getBytes("UTF-8");
            } catch (Exception ex) {
                return null;
            }
        }

        @Override
        public Value dispatch(String key, Update update, Function function) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean dispatch(String key, Predicate predicate, Condition condition) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
