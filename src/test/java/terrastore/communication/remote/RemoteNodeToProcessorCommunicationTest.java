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
package terrastore.communication.remote;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.Before;
import org.junit.Test;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.remote.pipe.Topology;
import terrastore.communication.protocol.Command;
import terrastore.communication.protocol.GetValueCommand;
import terrastore.communication.protocol.Response;
import terrastore.communication.serialization.JavaSerializer;
import terrastore.store.Bucket;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import static org.easymock.classextension.EasyMock.*;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class RemoteNodeToProcessorCommunicationTest {

    private static final String JSON_VALUE = "{\"test\":\"test\"}";
    private Topology pipes;

    @Before
    public void onSetUp() {
        pipes = new Topology(new JavaSerializer<Command>(), new JavaSerializer<Response>());
    }

    @Test
    public void testSendProcessAndReceive() throws StoreOperationException, ProcessingException {
        String nodeName = "node";
        String bucketName = "bucket";
        String valueKey = "key";
        Value value = new Value(JSON_VALUE.getBytes());
        Map<String, Value> values = new HashMap<String, Value>();
        values.put(valueKey, value);

        Store store = createMock(Store.class);
        Bucket bucket = createMock(Bucket.class);

        store.get(bucketName);
        expectLastCall().andReturn(bucket).once();
        bucket.get(valueKey);
        expectLastCall().andReturn(value).once();

        replay(store, bucket);

        Node sender = new RemoteNode(nodeName, pipes, 1000);
        RemoteProcessor processor = new RemoteProcessor(nodeName, pipes, store, Executors.newCachedThreadPool());
        Command command = new GetValueCommand(bucketName, valueKey);

        try {
            sender.connect();
            processor.start();

            Map<String, Value> result = sender.send(command);
            assertEquals(1, result.size());
            assertEquals(new String(value.getBytes()), new String(result.get(valueKey).getBytes()));
        } finally {
            try {
                sender.disconnect();
                processor.stop();
            } finally {
                verify(store, bucket);
            }
        }
    }

    @Test(expected = ProcessingException.class)
    public void testCommunicationTimeout() throws StoreOperationException, ProcessingException {
        String nodeName = "node";
        String bucketName = "bucket";
        String valueKey = "key";
        Value value = new Value(JSON_VALUE.getBytes());
        Map<String, Value> values = new HashMap<String, Value>();
        values.put(valueKey, value);

        Store store = createMock(Store.class);
        Bucket bucket = createMock(Bucket.class);

        store.get(bucketName);
        expectLastCall().andReturn(bucket).anyTimes();
        bucket.get(valueKey);
        expectLastCall().andReturn(value).anyTimes();

        replay(store, bucket);

        Node sender = new RemoteNode(nodeName, pipes, 1000);
        RemoteProcessor processor = new RemoteProcessor(nodeName, pipes, store, Executors.newCachedThreadPool());
        Command command = new GetValueCommand(bucketName, valueKey);

        try {
            sender.connect();

            processor.start();
            processor.stop();
            
            sender.send(command);
        } finally {
            try {
                sender.disconnect();
            } finally {
                verify(store, bucket);
            }
        }
    }
}
