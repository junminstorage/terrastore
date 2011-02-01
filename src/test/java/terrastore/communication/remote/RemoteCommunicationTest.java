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
package terrastore.communication.remote;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.easymock.classextension.EasyMock;
import org.junit.Test;
import terrastore.communication.NodeConfiguration;
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.protocol.GetValueCommand;
import terrastore.communication.protocol.GetValuesCommand;
import terrastore.router.Router;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.util.collect.Maps;
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
        Key valueKey = new Key("key");
        Value value = new Value(VALUE.getBytes());

        Router router = createMock(Router.class);
        Node node = createMock(Node.class);

        router.routeToNodeFor(bucketName, valueKey);
        expectLastCall().andReturn(node).once();
        node.send(EasyMock.<GetValueCommand>anyObject());
        expectLastCall().andReturn(value).once();

        replay(router, node);

        RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9990, 10, false, router);
        Node sender = new RemoteNode(new NodeConfiguration(nodeName, "localhost", 9990, "localhost", 8000), 60000, false);
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
        final Key valueKey = new Key("key");
        final Value value = new Value(VALUE.getBytes());

        final Router router = createMock(Router.class);
        final Node node = createMock(Node.class);
        makeThreadSafe(router, true);
        makeThreadSafe(node, true);

        router.routeToNodeFor(bucketName, valueKey);
        expectLastCall().andReturn(node).anyTimes();
        node.send(EasyMock.<GetValueCommand>anyObject());
        expectLastCall().andReturn(value).anyTimes();

        replay(router, node);

        final RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9990, 10, false, router);
        processor.start();
        final RemoteNode sender = new RemoteNode(new NodeConfiguration(nodeName, "localhost", 9990, "localhost", 8000), 60000, false);
        sender.connect();
        try {
            final AtomicBoolean corrupted = new AtomicBoolean(false);
            final AtomicBoolean failed = new AtomicBoolean(false);
            final int threads = 100;
            final int times = 100;
            final ExecutorService executor = Executors.newFixedThreadPool(threads);
            long start = System.currentTimeMillis();
            for (int i = 0; i < times && corrupted.get() == false && failed.get() == false; i++) {
                executor.submit(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            GetValueCommand command = new GetValueCommand(bucketName, valueKey);
                            Value result = sender.<Value>send(command);
                            if (result == null || !Arrays.equals(value.getBytes(), result.getBytes())) {
                                corrupted.set(true);
                            }
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            failed.set(true);
                        }
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.SECONDS);
            if (corrupted.get()) {
                fail("Corrupted data!");
            }
            if (failed.get()) {
                fail("Failed!");
            }
            System.out.println("---> Elapsed: " + (System.currentTimeMillis() - start));
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
    public void testNodeCanAutomaticallyConnect() throws Exception {
        String nodeName = "node";
        String bucketName = "bucket";
        Key valueKey = new Key("key");
        Value value = new Value(VALUE.getBytes());

        Router router = createMock(Router.class);
        Node node = createMock(Node.class);

        router.routeToNodeFor(bucketName, valueKey);
        expectLastCall().andReturn(node).anyTimes();
        node.send(EasyMock.<GetValueCommand>anyObject());
        expectLastCall().andReturn(value).anyTimes();

        replay(router, node);

        RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9991, 10, false, router);
        Node sender = new RemoteNode(new NodeConfiguration(nodeName, "localhost", 9991, "localhost", 8000), 1000, false);
        GetValueCommand command = new GetValueCommand(bucketName, valueKey);

        try {
            // Start processor
            processor.start();
            // Try to send:
            sender.<Value>send(command);
        } finally {
            sender.disconnect();
            processor.stop();
            verify(router, node);
        }
    }

    @Test
    public void testNodeCanReconnect() throws Exception {
        String nodeName = "node";
        String bucketName = "bucket";
        Key valueKey = new Key("key");
        Value value = new Value(VALUE.getBytes());

        Router router = createMock(Router.class);
        Node node = createMock(Node.class);

        router.routeToNodeFor(bucketName, valueKey);
        expectLastCall().andReturn(node).anyTimes();
        node.send(EasyMock.<GetValueCommand>anyObject());
        expectLastCall().andReturn(value).anyTimes();

        replay(router, node);

        RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9991, 10, false, router);
        Node sender = new RemoteNode(new NodeConfiguration(nodeName, "localhost", 9991, "localhost", 8000), 1000, false);
        GetValueCommand command = new GetValueCommand(bucketName, valueKey);

        try {
            // Start processor
            processor.start();
            // Connect node:
            sender.connect();
            // Disconnect node:
            sender.disconnect();
            // Reconnect:
            sender.connect();
            // Try to send:
            sender.<Value>send(command);
        } finally {
            processor.stop();
            verify(router, node);
        }
    }

    @Test
    public void testNodeCanReconnectAfterException() throws Exception {
        String nodeName = "node";
        String bucketName = "bucket";
        Key valueKey = new Key("key");
        Value value = new Value(VALUE.getBytes());

        Router router = createMock(Router.class);
        Node node = createMock(Node.class);

        router.routeToNodeFor(bucketName, valueKey);
        expectLastCall().andReturn(node).anyTimes();
        node.send(EasyMock.<GetValueCommand>anyObject());
        expectLastCall().andReturn(value).anyTimes();

        replay(router, node);

        RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9991, 10, false, router);
        Node sender = new RemoteNode(new NodeConfiguration(nodeName, "localhost", 9991, "localhost", 8000), 1000, false);
        GetValueCommand command = new GetValueCommand(bucketName, valueKey);

        try {
            // Try to connect node:
            sender.connect();
        } catch (Exception ex) {
            // Disconnect node:
            sender.disconnect();
            // Start processor
            processor.start();
            // Reconnect:
            sender.connect();
            // Try to send:
            sender.<Value>send(command);
        } finally {
            processor.stop();
            verify(router, node);
        }
    }

    @Test
    public void testWithLargeDataSet() throws Exception {
        String nodeName = "node";
        String bucketName = "bucket";
        Set<Key> keys = new HashSet<Key>();
        for (int i = 0; i < 100000; i++) {
            keys.add(new Key(i + new String(new byte[100]) + i));
        }

        Router router = createMock(Router.class);
        Node node = createMock(Node.class);
        makeThreadSafe(router, true);
        makeThreadSafe(node, true);
        router.routeToNodesFor(eq(bucketName), eq(keys));
        expectLastCall().andReturn(Maps.hash(new Node[]{node}, new Set[]{keys})).anyTimes();
        node.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(Collections.EMPTY_MAP).anyTimes();

        replay(router, node);

        RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9991, 10, false, router);
        Node sender = new RemoteNode(new NodeConfiguration(nodeName, "localhost", 9991, "localhost", 8000), 60000, false);

        GetValuesCommand command = new GetValuesCommand(bucketName, keys);

        try {
            // Start processor
            processor.start();
            // Connect node:
            sender.connect();
            // Try to send:
            sender.<Value>send(command);
        } finally {
            verify(router, node);
            processor.stop();
        }
    }

    @Test(expected = CommunicationException.class)
    public void testCommunicationError() throws Exception {
        String nodeName = "node";
        String bucketName = "bucket";
        Key valueKey = new Key("key");

        Router router = createMock(Router.class);
        Node node = createMock(Node.class);

        replay(router, node);

        RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9991, 10, false, router);
        Node sender = new RemoteNode(new NodeConfiguration(nodeName, "localhost", 9991, "localhost", 8000), 1000, false);
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

    @Test
    public void testBindAndConnectToAnyHost() throws Exception {
        Router router = createMock(Router.class);
        Node node = createMock(Node.class);

        replay(router, node);

        RemoteProcessor processor = new RemoteProcessor("0.0.0.0", 9991, 10, false, router);
        Node sender = new RemoteNode(new NodeConfiguration("node", "0.0.0.0", 9991, "localhost", 8000), 1000, false);

        try {
            // Start processor
            processor.start();
            // Connect node:
            sender.connect();
        } finally {
            sender.disconnect();
            processor.stop();
            verify(router, node);
        }
    }
}
