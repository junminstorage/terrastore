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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import org.easymock.classextension.EasyMock;
import org.junit.Test;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.GetValueCommand;
import terrastore.store.Bucket;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
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
    public void testSendProcessAndReceive() throws StoreOperationException, ProcessingException {
        String nodeName = "node";
        String bucketName = "bucket";
        String valueKey = "key";
        Value value = new TestValue(VALUE);
        Map<String, Value> values = new HashMap<String, Value>();
        values.put(valueKey, value);

        Store store = createMock(Store.class);
        Bucket bucket = createMock(Bucket.class);
        Node backup = createMock(Node.class);

        store.get(bucketName);
        expectLastCall().andReturn(bucket).once();
        bucket.get(valueKey);
        expectLastCall().andReturn(value).once();

        replay(store, bucket, backup);

        RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9990, store, 10);
        Node sender = new RemoteNode("127.0.0.1", 9990, nodeName, 1000, backup);
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
                verify(store, bucket, backup);
            }
        }
    }

    @Test(expected = ProcessingException.class)
    public void testCommunicationTimeout() throws StoreOperationException, ProcessingException {
        String nodeName = "node";
        String bucketName = "bucket";
        String valueKey = "key";
        Value value = new TestValue(VALUE);
        Map<String, Value> values = new HashMap<String, Value>();
        values.put(valueKey, value);

        Store store = createMock(Store.class);
        Bucket bucket = createMock(Bucket.class);
        Node backup = createMock(Node.class);

        store.get(bucketName);
        expectLastCall().andReturn(bucket).anyTimes();
        bucket.get(valueKey);
        expectLastCall().andReturn(value).anyTimes();

        replay(store, bucket, backup);

        RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9991, store, 10);
        Node sender = new RemoteNode("127.0.0.1", 9991, nodeName, 1000, backup);
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
            verify(store, bucket, backup);
        }
    }

    @Test
    public void testReroutingOnDisconnection() throws Exception {
        String nodeName = "node";
        String bucketName = "bucket";
        String valueKey = "key";
        Value value = new TestValue(VALUE);
        Map<String, Value> values = new HashMap<String, Value>();
        values.put(valueKey, value);

        Store store = createMock(Store.class);
        Bucket bucket = createMock(Bucket.class);
        Node backup = createMock(Node.class);
        makeThreadSafe(backup, true);

        store.get(bucketName);
        expectLastCall().andReturn(bucket).anyTimes();
        bucket.get(valueKey);
        expectLastCall().andReturn(value).anyTimes();
        backup.send(EasyMock.<GetValueCommand>anyObject());
        expectLastCall().andReturn(value).once();

        replay(store, bucket, backup);

        final RemoteProcessor processor = new RemoteProcessor("127.0.0.1", 9992, store, 10);
        final Node sender = new RemoteNode("127.0.0.1", 9992, nodeName, 10000, backup);
        final GetValueCommand command = new GetValueCommand(bucketName, valueKey);

        try {
            // Start processor
            processor.start();
            // Connect node:
            sender.connect();
            // Stop processor so that no communication can happen:
            processor.stop();
            // Try to send in another thread so that we can disconnect and reroute:
            Executors.newSingleThreadExecutor().submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        sender.<Value>send(command);
                    } catch (Exception ex) {
                    }
                }
            });
        } finally {
            Thread.sleep(1000);
            sender.disconnect();
        }

        verify(store, bucket, backup);
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
