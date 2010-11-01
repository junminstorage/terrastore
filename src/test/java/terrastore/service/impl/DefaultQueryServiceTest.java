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
package terrastore.service.impl;

import java.util.Arrays;
import java.util.Collection;
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
import terrastore.communication.protocol.GetKeysCommand;
import terrastore.communication.protocol.KeysInRangeCommand;
import terrastore.communication.protocol.GetBucketsCommand;
import terrastore.communication.protocol.GetValueCommand;
import terrastore.communication.protocol.GetValuesCommand;
import terrastore.communication.protocol.MapCommand;
import terrastore.communication.protocol.ReduceCommand;
import terrastore.router.Router;
import terrastore.service.QueryOperationException;
import terrastore.store.Key;
import terrastore.store.features.Predicate;
import terrastore.store.features.Range;
import terrastore.store.Value;
import terrastore.store.features.Mapper;
import terrastore.store.features.Reducer;
import terrastore.util.collect.Maps;
import terrastore.util.collect.Sets;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class DefaultQueryServiceTest {

    private static final String JSON_VALUE = "{\"test\":\"test\"}";
    private static final String REDUCE_VALUE = "{\"k1\":\"v1\",\"k2\":\"v2\"}";

    @Test
    public void testGetBuckets() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Cluster cluster2 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Sets.linked(node1), Sets.linked(node2)})).once();

        node1.send(EasyMock.<GetBucketsCommand>anyObject());
        expectLastCall().andReturn(Sets.hash("test1")).once();
        node2.send(EasyMock.<GetBucketsCommand>anyObject());
        expectLastCall().andReturn(Sets.hash("test2")).once();

        replay(cluster1, cluster2, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);
        Collection<String> result = service.getBuckets();
        assertEquals(2, result.size());
        assertTrue(result.contains("test1"));
        assertTrue(result.contains("test2"));

        verify(cluster1, cluster2, node1, node2, router);
    }

    @Test(expected = QueryOperationException.class)
    public void testGetBucketsFailsInCaseOfProcessingException() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        Node node2 = createMock(Node.class);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<GetBucketsCommand>anyObject());
        expectLastCall().andThrow(new ProcessingException(new ErrorMessage(0, ""))).once();

        replay(cluster1, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);
        try {
            Collection<String> result = service.getBuckets();
        } finally {
            verify(cluster1, node1, node2, router);
        }
    }

    @Test
    public void testGetBucketsSucceedsBySkippingFailingNodes() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        Node node2 = createMock(Node.class);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<GetBucketsCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();
        node2.send(EasyMock.<GetBucketsCommand>anyObject());
        expectLastCall().andReturn(Sets.hash("test1", "test2")).once();

        replay(cluster1, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);
        Collection<String> result = service.getBuckets();
        assertEquals(2, result.size());
        assertTrue(result.contains("test1"));
        assertTrue(result.contains("test2"));

        verify(cluster1, node1, node2, router);
    }

    @Test
    public void testGetBucketsIgnoresAllNodesFailing() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        Node node2 = createMock(Node.class);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();
        node2.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();

        replay(cluster1, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);
        assertTrue(service.getBuckets().isEmpty());

        verify(cluster1, node1, node2, router);
    }

    @Test
    public void testGetBucketsIgnoresClusterWithNoNodes() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Cluster cluster2 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        Node node2 = createMock(Node.class);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Collections.emptySet(), Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<GetBucketsCommand>anyObject());
        expectLastCall().andReturn(Sets.hash("test1", "test2")).once();

        replay(cluster1, cluster2, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);
        Collection<String> result = service.getBuckets();
        assertEquals(2, result.size());
        assertTrue(result.contains("test1"));
        assertTrue(result.contains("test2"));

        verify(cluster1, cluster2, node1, node2, router);
    }

    @Test
    public void testGetValue() throws Exception {
        Value value = new Value(JSON_VALUE.getBytes());

        Node node = createMock(Node.class);
        Router router = createMock(Router.class);

        router.routeToNodeFor("bucket", new Key("test1"));
        expectLastCall().andReturn(node).once();
        node.send(EasyMock.<GetValueCommand>anyObject());
        expectLastCall().andReturn(value).once();

        replay(node, router);

        DefaultQueryService service = new DefaultQueryService(router);
        assertEquals(JSON_VALUE, new String(service.getValue("bucket", new Key("test1"), new Predicate(null)).getBytes()));

        verify(node, router);
    }

    @Test
    public void testGetAllValues() throws Exception {
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
        Map<Key, Value> values1 = new HashMap<Key, Value>();
        values1.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        Map<Key, Value> values2 = new HashMap<Key, Value>();
        values2.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Sets.hash(node1), Sets.hash(node2)})).once();

        node1.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"))).once();
        node2.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node1.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values1).once();
        node2.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values2).once();

        replay(cluster1, cluster2, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);
        Map<Key, Value> result = service.getAllValues("bucket", 0);
        assertEquals(2, result.size());
        assertEquals(JSON_VALUE, new String(result.get(new Key("test1")).getBytes()));
        assertEquals(JSON_VALUE, new String(result.get(new Key("test2")).getBytes()));

        verify(cluster1, cluster2, node1, node2, router);
    }

    @Test
    public void testGetAllValuesSucceedsBySkippingFailingNodes() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);
        Map<Node, Set<Key>> nodeToKeys = new HashMap<Node, Set<Key>>();
        nodeToKeys.put(node2, new HashSet<Key>(Arrays.asList(new Key("test1"), new Key("test2"))));
        Map<Key, Value> values = new HashMap<Key, Value>();
        values.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        values.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();
        node2.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"), new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node2.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values).once();

        replay(cluster1, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);
        Map<Key, Value> result = service.getAllValues("bucket", 0);
        assertEquals(2, result.size());
        assertEquals(JSON_VALUE, new String(result.get(new Key("test1")).getBytes()));
        assertEquals(JSON_VALUE, new String(result.get(new Key("test2")).getBytes()));

        verify(cluster1, node1, node2, router);
    }

    @Test(expected = QueryOperationException.class)
    public void testGetAllValuesFailsInCaseOfProcessingException() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andThrow(new ProcessingException(new ErrorMessage(0, ""))).once();

        replay(cluster1, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);
        try {
            Map<Key, Value> result = service.getAllValues("bucket", 0);
        } finally {
            verify(cluster1, node1, node2, router);
        }
    }

    @Test
    public void testGetAllValuesIgnoresAllNodesFailing() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();
        node2.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();

        router.routeToNodesFor("bucket", Sets.<Key>hash());
        expectLastCall().andReturn(Maps.hash(new Node[0], new Set[0])).once();

        replay(cluster1, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);
        assertTrue(service.getAllValues("bucket", 0).isEmpty());

        verify(cluster1, node1, node2, router);
    }

    @Test
    public void testGetAllValuesIgnoresClusterWithNoNodes() throws Exception {
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
        Map<Key, Value> values1 = new HashMap<Key, Value>();
        values1.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        Map<Key, Value> values2 = new HashMap<Key, Value>();
        values2.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Collections.emptySet(), Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"), new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node1.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values1).once();
        node2.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values2).once();

        replay(cluster1, cluster2, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);
        Map<Key, Value> result = service.getAllValues("bucket", 0);
        assertEquals(2, result.size());
        assertEquals(JSON_VALUE, new String(result.get(new Key("test1")).getBytes()));
        assertEquals(JSON_VALUE, new String(result.get(new Key("test2")).getBytes()));

        verify(cluster1, cluster2, node1, node2, router);
    }

    @Test
    public void testQueryByRange() throws Exception {
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
        Map<Key, Value> values1 = new HashMap<Key, Value>();
        values1.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        Map<Key, Value> values2 = new HashMap<Key, Value>();
        values2.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Sets.hash(node1), Sets.hash(node2)})).once();

        node1.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"))).once();
        node2.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node1.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values1).once();
        node2.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values2).once();

        replay(cluster1, cluster2, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);

        Map<Key, Value> result = service.queryByRange("bucket", new Range(new Key("test1"), new Key("test2"), 0, "order", 0), new Predicate(null));
        assertEquals(2, result.size());
        assertEquals(new Key("test1"), result.keySet().toArray()[0]);
        assertEquals(new Key("test2"), result.keySet().toArray()[1]);

        verify(cluster1, cluster2, node1, node2, router);
    }

    @Test(expected = QueryOperationException.class)
    public void testQueryByRangeFailsInCaseOfProcessingException() throws Exception {
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


        DefaultQueryService service = new DefaultQueryService(router);
        try {
            Map<Key, Value> result = service.queryByRange("bucket", new Range(new Key("test1"), new Key("test2"), 0, "order", 0), new Predicate(null));
        } finally {
            verify(cluster1, node1, node2, router);
        }
    }

    @Test
    public void testQueryByRangeSucceedsBySkippingFailingNodes() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);
        Map<Node, Set<Key>> nodeToKeys = new HashMap<Node, Set<Key>>();
        nodeToKeys.put(node2, new HashSet<Key>(Arrays.asList(new Key("test1"), new Key("test2"))));
        Map<Key, Value> values = new HashMap<Key, Value>();
        values.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        values.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();
        node2.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"), new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node2.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values).once();

        replay(cluster1, node1, node2, router);


        DefaultQueryService service = new DefaultQueryService(router);
        Map<Key, Value> result = service.queryByRange("bucket", new Range(new Key("test1"), new Key("test2"), 0, "order", 0), new Predicate(null));
        assertEquals(2, result.size());
        assertEquals(JSON_VALUE, new String(result.get(new Key("test1")).getBytes()));
        assertEquals(JSON_VALUE, new String(result.get(new Key("test2")).getBytes()));

        verify(cluster1, node1, node2, router);
    }

    @Test
    public void testQueryByRangeIgnoresAllNodesFailing() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();
        node2.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();

        router.routeToNodesFor("bucket", Sets.<Key>hash());
        expectLastCall().andReturn(Maps.hash(new Node[0], new Set[0])).once();

        replay(cluster1, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);
        assertTrue(service.queryByRange("bucket", new Range(new Key("test1"), new Key("test2"), 0, "order", 0), new Predicate(null)).isEmpty());

        verify(cluster1, node1, node2, router);
    }

    @Test
    public void testQueryByRangeIgnoresClusterWithNoNodes() throws Exception {
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
        Map<Key, Value> values1 = new HashMap<Key, Value>();
        values1.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        Map<Key, Value> values2 = new HashMap<Key, Value>();
        values2.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Collections.emptySet(), Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Sets.linked(new Key("test1"), new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node1.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values1).once();
        node2.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values2).once();

        replay(cluster1, cluster2, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);

        Map<Key, Value> result = service.queryByRange("bucket", new Range(new Key("test1"), new Key("test2"), 0, "order", 0), new Predicate(null));
        assertEquals(2, result.size());
        assertEquals(new Key("test1"), result.keySet().toArray()[0]);
        assertEquals(new Key("test2"), result.keySet().toArray()[1]);

        verify(cluster1, cluster2, node1, node2, router);
    }

    @Test
    public void testQueryByPredicate() throws Exception {
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
        Map<Key, Value> values1 = new HashMap<Key, Value>();
        values1.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        Map<Key, Value> values2 = new HashMap<Key, Value>();
        values2.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Sets.hash(node1), Sets.hash(node2)})).once();

        node1.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"))).once();
        node2.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node1.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values1).once();
        node2.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values2).once();

        replay(cluster1, cluster2, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);

        Map<Key, Value> result = service.queryByPredicate("bucket", new Predicate("test:true"));
        assertEquals(2, result.size());
        assertTrue(result.containsKey(new Key("test1")));
        assertTrue(result.containsKey(new Key("test2")));

        verify(cluster1, cluster2, node1, node2, router);
    }

    @Test
    public void testQueryByPredicateSucceedsBySkippingFailingNodes() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);
        Map<Node, Set<Key>> nodeToKeys = new HashMap<Node, Set<Key>>();
        nodeToKeys.put(node2, new HashSet<Key>(Arrays.asList(new Key("test1"), new Key("test2"))));
        Map<Key, Value> values = new HashMap<Key, Value>();
        values.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        values.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();
        node2.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"), new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node2.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values).once();

        replay(cluster1, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);

        Map<Key, Value> result = service.queryByPredicate("bucket", new Predicate("test:true"));
        assertEquals(2, result.size());
        assertEquals(JSON_VALUE, new String(result.get(new Key("test1")).getBytes()));
        assertEquals(JSON_VALUE, new String(result.get(new Key("test2")).getBytes()));

        verify(cluster1, node1, node2, router);
    }

    @Test
    public void testQueryByPredicateIgnoresAllNodesFailing() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1}, new Set[]{Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();
        node2.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();

        router.routeToNodesFor("bucket", Sets.<Key>hash());
        expectLastCall().andReturn(Maps.hash(new Node[0], new Set[0])).once();

        replay(cluster1, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);

        assertTrue(service.queryByPredicate("bucket", new Predicate("test:true")).isEmpty());

        verify(cluster1, node1, node2, router);
    }

    @Test
    public void testQueryByPredicateIgnoresClusterWithNoNodes() throws Exception {
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
        Map<Key, Value> values1 = new HashMap<Key, Value>();
        values1.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        Map<Key, Value> values2 = new HashMap<Key, Value>();
        values2.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Collections.emptySet(), Sets.linked(node1, node2)})).once();

        node1.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"), new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node1.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values1).once();
        node2.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values2).once();

        replay(cluster1, cluster2, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);

        Map<Key, Value> result = service.queryByPredicate("bucket", new Predicate("test:true"));
        assertEquals(2, result.size());
        assertTrue(result.containsKey(new Key("test1")));
        assertTrue(result.containsKey(new Key("test2")));

        verify(cluster1, cluster2, node1, node2, router);
    }

    @Test
    public void testQueryByMapReduceWithRange() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Cluster cluster2 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);
        Map<Node, Set<Key>> nodeToKeys = new HashMap<Node, Set<Key>>();
        nodeToKeys.put(node1, Sets.hash(new Key("test1")));
        nodeToKeys.put(node2, Sets.hash(new Key("test2")));
        Map<String, Object> mapResult1 = new HashMap<String, Object>();
        mapResult1.put("k1", "v1");
        Map<String, Object> mapResult2 = new HashMap<String, Object>();
        mapResult2.put("k2", "v2");
        Value reduceResult = new Value(REDUCE_VALUE.getBytes());

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Sets.hash(node1), Sets.hash(node2)})).once();
        router.routeToLocalNode();
        expectLastCall().andReturn(node1).once();

        node1.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"))).once();
        node2.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node1.send(EasyMock.<MapCommand>anyObject());
        expectLastCall().andReturn(mapResult1).once();
        node2.send(EasyMock.<MapCommand>anyObject());
        expectLastCall().andReturn(mapResult2).once();
        node1.send(EasyMock.<ReduceCommand>anyObject());
        expectLastCall().andReturn(reduceResult).once();

        replay(cluster1, cluster2, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);

        Range range = new Range(new Key("k1"), null, 0, null, 1000);
        Mapper mapper = new Mapper("mapper", null, 1000, null);
        Reducer reducer = new Reducer("reducer", 1000);
        Value result = service.queryByMapReduce("bucket", range, mapper, reducer);
        assertEquals(new Value(REDUCE_VALUE.getBytes()), result);

        verify(cluster1, cluster2, node1, node2, router);
    }

    @Test
    public void testQueryByMapReduceWithFullScan() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Cluster cluster2 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);
        Map<Node, Set<Key>> nodeToKeys = new HashMap<Node, Set<Key>>();
        nodeToKeys.put(node1, Sets.hash(new Key("test1")));
        nodeToKeys.put(node2, Sets.hash(new Key("test2")));
        Map<String, Object> mapResult1 = new HashMap<String, Object>();
        mapResult1.put("k1", "v1");
        Map<String, Object> mapResult2 = new HashMap<String, Object>();
        mapResult2.put("k2", "v2");
        Value reduceResult = new Value(REDUCE_VALUE.getBytes());

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Sets.hash(node1), Sets.hash(node2)})).once();
        router.routeToLocalNode();
        expectLastCall().andReturn(node1).once();

        node1.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"))).once();
        node2.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test2"))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node1.send(EasyMock.<MapCommand>anyObject());
        expectLastCall().andReturn(mapResult1).once();
        node2.send(EasyMock.<MapCommand>anyObject());
        expectLastCall().andReturn(mapResult2).once();
        node1.send(EasyMock.<ReduceCommand>anyObject());
        expectLastCall().andReturn(reduceResult).once();

        replay(cluster1, cluster2, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);

        Mapper mapper = new Mapper("mapper", null, 1000, null);
        Reducer reducer = new Reducer("reducer", 1000);
        Value result = service.queryByMapReduce("bucket", null, mapper, reducer);
        assertEquals(new Value(REDUCE_VALUE.getBytes()), result);

        verify(cluster1, cluster2, node1, node2, router);
    }

    @Test
    public void testQueryByMapReduceSucceedsBySkippingFailingNodesDuringKeysHarvesting() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Cluster cluster2 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);
        Map<Node, Set<Key>> nodeToKeys = new HashMap<Node, Set<Key>>();
        nodeToKeys.put(node1, Sets.hash(new Key("test1"), new Key("test2")));
        Map<String, Object> mapResult = Maps.hash(new String[]{"k1", "k2"}, new Object[]{"v1", "v2"});
        Value reduceResult = new Value(REDUCE_VALUE.getBytes());

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Sets.linked(node1), Sets.hash(node2)})).once();
        router.routeToLocalNode();
        expectLastCall().andReturn(node1).once();

        node1.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"), new Key("test2"))).once();
        node2.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(0, ""))).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node1.send(EasyMock.<MapCommand>anyObject());
        expectLastCall().andReturn(mapResult).once();
        node1.send(EasyMock.<ReduceCommand>anyObject());
        expectLastCall().andReturn(reduceResult).once();

        replay(cluster1, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);

        Range range = new Range(new Key("k1"), null, 0, null, 1000);
        Mapper mapper = new Mapper("mapper", null, 1000, null);
        Reducer reducer = new Reducer("reducer", 1000);
        Value result = service.queryByMapReduce("bucket", range, mapper, reducer);
        assertEquals(new Value(REDUCE_VALUE.getBytes()), result);

        verify(cluster1, node1, node2, router);
    }

    @Test(expected = QueryOperationException.class)
    public void testQueryByMapReduceFailsOnFailingNodeDuringMapPhase() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Cluster cluster2 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);
        Map<Node, Set<Key>> nodeToKeys = new HashMap<Node, Set<Key>>();
        nodeToKeys.put(node1, Sets.hash(new Key("test1"), new Key("test2")));

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Sets.linked(node1), Sets.hash(node2)})).once();

        node1.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"), new Key("test2"))).once();
        node2.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Collections.EMPTY_SET).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node1.send(EasyMock.<MapCommand>anyObject());
        expectLastCall().andThrow(new ProcessingException(new ErrorMessage(0, ""))).once();

        replay(cluster1, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);

        Range range = new Range(new Key("k1"), null, 0, null, 1000);
        Mapper mapper = new Mapper("mapper", null, 1000, null);
        Reducer reducer = new Reducer("reducer", 1000);
        try {
            service.queryByMapReduce("bucket", range, mapper, reducer);
        } finally {
            verify(cluster1, node1, node2, router);
        }
    }

    @Test(expected = QueryOperationException.class)
    public void testQueryByMapReduceFailsOnFailingNodeDuringReducePhase() throws Exception {
        Cluster cluster1 = createMock(Cluster.class);
        Cluster cluster2 = createMock(Cluster.class);
        Node node1 = createMock(Node.class);
        makeThreadSafe(node1, true);
        Node node2 = createMock(Node.class);
        makeThreadSafe(node2, true);
        Router router = createMock(Router.class);
        Map<Node, Set<Key>> nodeToKeys = new HashMap<Node, Set<Key>>();
        nodeToKeys.put(node1, Sets.hash(new Key("test1"), new Key("test2")));
        Map<String, Object> mapResult = Maps.hash(new String[]{"k1", "k2"}, new Object[]{"v1", "v2"});

        router.broadcastRoute();
        expectLastCall().andReturn(Maps.hash(new Cluster[]{cluster1, cluster2}, new Set[]{Sets.linked(node1), Sets.hash(node2)})).once();
        router.routeToLocalNode();
        expectLastCall().andReturn(node1).once();

        node1.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Sets.hash(new Key("test1"), new Key("test2"))).once();
        node2.send(EasyMock.<KeysInRangeCommand>anyObject());
        expectLastCall().andReturn(Collections.EMPTY_SET).once();

        router.routeToNodesFor("bucket", Sets.hash(new Key("test1"), new Key("test2")));
        expectLastCall().andReturn(nodeToKeys).once();

        node1.send(EasyMock.<MapCommand>anyObject());
        expectLastCall().andReturn(mapResult).once();
        node1.send(EasyMock.<ReduceCommand>anyObject());
        expectLastCall().andThrow(new ProcessingException(new ErrorMessage(0, ""))).once();

        replay(cluster1, node1, node2, router);

        DefaultQueryService service = new DefaultQueryService(router);

        Range range = new Range(new Key("k1"), null, 0, null, 1000);
        Mapper mapper = new Mapper("mapper", null, 1000, null);
        Reducer reducer = new Reducer("reducer", 1000);
        try {
            service.queryByMapReduce("bucket", range, mapper, reducer);
        } finally {
            verify(cluster1, node1, node2, router);
        }
    }

}
