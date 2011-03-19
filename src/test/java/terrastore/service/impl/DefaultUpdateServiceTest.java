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
package terrastore.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.easymock.classextension.EasyMock;
import org.junit.Test;
import terrastore.common.ErrorMessage;
import terrastore.communication.Cluster;
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.BulkPutCommand;
import terrastore.communication.protocol.KeysInRangeCommand;
import terrastore.communication.protocol.MergeCommand;
import terrastore.communication.protocol.PutValueCommand;
import terrastore.communication.protocol.RemoveBucketCommand;
import terrastore.communication.protocol.RemoveValueCommand;
import terrastore.communication.protocol.RemoveValuesCommand;
import terrastore.communication.protocol.UpdateCommand;
import terrastore.router.Router;
import terrastore.server.Keys;
import terrastore.server.Values;
import terrastore.service.UpdateOperationException;
import terrastore.store.Key;
import terrastore.store.ValidationException;
import terrastore.store.features.Predicate;
import terrastore.store.features.Range;
import terrastore.store.features.Update;
import terrastore.store.Value;
import terrastore.util.collect.Maps;
import terrastore.util.collect.Sets;
import static org.easymock.classextension.EasyMock.*;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class DefaultUpdateServiceTest {

    private static final String JSON_VALUE = "{\"test\":\"test\"}";
    private static final String JSON_VALUES = "{\"test1\":" + JSON_VALUE + ",\"test2\":" + JSON_VALUE + "}";
    private static final String BAD_JSON_VALUE = "{\"test\"\"test\"}";

    @Test
    public void testRemoveBucket() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Cluster cluster2 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        Node node2 = createMock(Node.class);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Sets.hash(node1), Sets.hash(node2)})).once();
        node1.send(EasyMock.<RemoveBucketCommand>anyObject());
        expectLastCall().andReturn(null).once();
        node2.send(EasyMock.<RemoveBucketCommand>anyObject());
        expectLastCall().andReturn(null).once();

        replay(cluster1, cluster2, node1, node2, router);

        DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
        service.removeBucket("bucket");

        verify(cluster1, cluster2, node1, node2, router);
    }

    @Test
    public void testRemoveBucketSucceedsBySkippingFailingNodes() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        Node node2 = createMock(Node.class);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();
        node1.send(EasyMock.<RemoveBucketCommand>anyObject());
        expectLastCall().andThrow(new ProcessingException(new ErrorMessage(0, ""))).once();
        node2.send(EasyMock.<RemoveBucketCommand>anyObject());
        expectLastCall().andReturn(null).once();

        replay(cluster1, node1, node2, router);

        DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
        service.removeBucket("bucket");

        verify(cluster1, node1, node2, router);
    }

    @Test(expected = CommunicationException.class)
    public void testRemoveBucketFailsWhenAllNodesFail() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        Node node2 = createMock(Node.class);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.hash(node1, node2)})).once();
        node1.send(EasyMock.<RemoveBucketCommand>anyObject());
        expectLastCall().andThrow(new ProcessingException(new ErrorMessage(0, ""))).once();
        node2.send(EasyMock.<RemoveBucketCommand>anyObject());
        expectLastCall().andThrow(new ProcessingException(new ErrorMessage(0, ""))).once();

        replay(cluster1, node1, node2, router);

        try {
            DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
            service.removeBucket("bucket");
        } finally {
            verify(cluster1, node1, node2, router);
        }
    }

    @Test(expected = CommunicationException.class)
    public void testRemoveBucketFailsWhenNoNodesForCluster() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Collections.emptySet()})).once();

        replay(cluster1, router);

        try {
            DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
            service.removeBucket("bucket");
        } finally {
            verify(cluster1, router);
        }
    }

    @Test
    public void testBulkPut() throws Exception {
        Node node = createMock(Node.class);
        Router router = createMock(Router.class);

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(Maps.hash(new Node[]{node}, new Set[]{Sets.hash(new Key("test1"), new Key("test2"))})).once();
        node.send(EasyMock.<BulkPutCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"), new Key("test2"))).once();

        replay(node, router);

        DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
        Keys result = service.bulkPut("bucket", new Values(Maps.hash(new Key[]{new Key("test1"), new Key("test2")}, new Value[]{new Value(JSON_VALUE.getBytes()), new Value(JSON_VALUE.getBytes())})));
        assertEquals(2, result.size());
        assertTrue(result.contains(new Key("test1")));
        assertTrue(result.contains(new Key("test2")));

        verify(node, router);
    }

    @Test
    public void testBulkPutIgnoresFailingNodes() throws Exception {
        Node goodNode = createMock(Node.class);
        Node badNode = createMock(Node.class);
        Router router = createMock(Router.class);

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(Maps.hash(new Node[]{goodNode, badNode}, new Set[]{Sets.hash(new Key("test1")), Sets.hash(new Key("test2"))})).once();
        goodNode.send(EasyMock.<BulkPutCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"))).once();
        badNode.send(EasyMock.<BulkPutCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage())).once();

        replay(goodNode, badNode, router);

        DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
        Keys result = service.bulkPut("bucket", new Values(Maps.hash(new Key[]{new Key("test1"), new Key("test2")}, new Value[]{new Value(JSON_VALUE.getBytes()), new Value(JSON_VALUE.getBytes())})));
        assertEquals(1, result.size());
        assertTrue(result.contains(new Key("test1")));
        assertFalse(result.contains(new Key("test2")));

        verify(goodNode, badNode, router);
    }

    @Test
    public void testPutValue() throws Exception {
        Node node = createMock(Node.class);
        Router router = createMock(Router.class);

        router.routeToNodeFor("bucket", new Key("test1"));
        expectLastCall().andReturn(node).once();
        node.send(EasyMock.<PutValueCommand>anyObject());
        expectLastCall().andReturn(null).once();

        replay(node, router);

        DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
        service.putValue("bucket", new Key("test1"), new Value(JSON_VALUE.getBytes()), new Predicate(null));

        verify(node, router);
    }

    @Test(expected = ValidationException.class)
    public void testPutBadValue() throws Exception {
        Router router = createMock(Router.class);

        replay(router);

        try {
            DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
            service.putValue("bucket", new Key("test1"), new Value(BAD_JSON_VALUE.getBytes()), new Predicate(null));
        } finally {
            verify(router);
        }
    }

    @Test
    public void testRemoveValue() throws Exception {
        Node node = createMock(Node.class);
        Router router = createMock(Router.class);

        router.routeToNodeFor("bucket", new Key("test1"));
        expectLastCall().andReturn(node).once();
        node.send(EasyMock.<RemoveValueCommand>anyObject());
        expectLastCall().andReturn(null).once();

        replay(node, router);

        DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
        service.removeValue("bucket", new Key("test1"));

        verify(node, router);
    }

    @Test
    public void testRemoveByRange() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Cluster cluster2 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);
        Map<Node, Set<Key>> nodeToKeys = new HashMap<Node, Set<Key>>();
        nodeToKeys.put(node1, new HashSet<Key>(Arrays.asList(new Key("test1"))));
        nodeToKeys.put(node2, new HashSet<Key>(Arrays.asList(new Key("test2"))));
        Set<Key> keys1 = new HashSet<Key>();
        keys1.add(new Key("test1"));
        Set<Key> keys2 = new HashSet<Key>();
        keys2.add(new Key("test2"));

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Sets.hash(node1), Sets.hash(node2)})).once();

        node1.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"))).once();
        node2.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node1.send(EasyMock.<RemoveValuesCommand>anyObject());
        expectLastCall().andReturn(keys1).once();
        node2.send(EasyMock.<RemoveValuesCommand>anyObject());
        expectLastCall().andReturn(keys2).once();

        replay(cluster1, cluster2, node1, node2, router);

        DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());

        Keys removedKeys = service.removeByRange("bucket", new Range(new Key("test1"), new Key("test2"), 0, "order", 0), new Predicate(null));
        assertEquals(2, removedKeys.size());
        assertTrue(removedKeys.contains(new Key("test1")));
        assertTrue(removedKeys.contains(new Key("test2")));

        verify(cluster1, cluster2, node1, node2, router);
    }

    @Test(expected = UpdateOperationException.class)
    public void testRemoveByRangeFailsInCaseOfProcessingException() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andThrow(new ProcessingException(new ErrorMessage(0, ""))).once();

        replay(cluster1, node1, node2, router);


        DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
        try {
            Keys removedKeys = service.removeByRange("bucket", new Range(new Key("test1"), new Key("test2"), 0, "order", 0), new Predicate(null));
        } finally {
            verify(cluster1, node1, node2, router);
        }
    }

    @Test
    public void testRemoveByRangeSucceedsBySkippingFailingNodes() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);

        Map<Node, Set<Key>> nodeToKeys = new HashMap<Node, Set<Key>>();
        nodeToKeys.put(node2, new HashSet<Key>(Arrays.asList(new Key("test1"), new Key("test2"))));
        Set<Key> keys2 = new HashSet<Key>();
        keys2.add(new Key("test1"));
        keys2.add(new Key("test2"));

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();
        node2.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"), new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node2.send(EasyMock.<RemoveValuesCommand>anyObject());
        expectLastCall().andReturn(keys2).once();

        replay(cluster1, node1, node2, router);

        DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
        Keys removedKeys = service.removeByRange("bucket", new Range(new Key("test1"), new Key("test2"), 0, "order", 0), new Predicate(null));
        assertEquals(2, removedKeys.size());
        assertTrue(removedKeys.contains(new Key("test1")));
        assertTrue(removedKeys.contains(new Key("test2")));

        verify(cluster1, node1, node2, router);
    }

    @Test
    public void testUpdateValue() throws Exception {
        Node node = createMock(Node.class);
        Router router = createMock(Router.class);

        router.routeToNodeFor("bucket", new Key("test1"));
        expectLastCall().andReturn(node).once();
        node.send(EasyMock.<UpdateCommand>anyObject());
        expectLastCall().andReturn(new Value(JSON_VALUE.getBytes())).once();

        replay(node, router);

        DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
        assertEquals(new Value(JSON_VALUE.getBytes()), service.updateValue("bucket", new Key("test1"), new Update("update", 1000, new HashMap<String, Object>())));

        verify(node, router);
    }

    @Test
    public void testMergeValue() throws Exception {
        Node node = createMock(Node.class);
        Router router = createMock(Router.class);

        router.routeToNodeFor("bucket", new Key("test1"));
        expectLastCall().andReturn(node).once();
        node.send(EasyMock.<MergeCommand>anyObject());
        expectLastCall().andReturn(new Value(JSON_VALUE.getBytes())).once();

        replay(node, router);

        DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
        assertEquals(new Value(JSON_VALUE.getBytes()), service.mergeValue("bucket", new Key("test1"), new Value("{}".getBytes())));

        verify(node, router);
    }

    @Test(expected = ValidationException.class)
    public void testMergeWithBadValue() throws Exception {
        Router router = createMock(Router.class);

        replay(router);

        try {
            DefaultUpdateService service = new DefaultUpdateService(router, new DefaultKeyRangeStrategy());
            service.mergeValue("bucket", new Key("test1"), new Value(BAD_JSON_VALUE.getBytes()));
        } finally {
            verify(router);
        }
    }
}
