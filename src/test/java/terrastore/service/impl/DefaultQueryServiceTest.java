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
package terrastore.service.impl;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.easymock.classextension.EasyMock;
import org.junit.Test;
import terrastore.communication.Node;
import terrastore.communication.protocol.GetKeysCommand;
import terrastore.communication.protocol.DoRangeQueryCommand;
import terrastore.communication.protocol.GetValueCommand;
import terrastore.communication.protocol.GetValuesCommand;
import terrastore.router.Router;
import terrastore.service.QueryOperationException;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.features.Range;
import terrastore.store.operators.Condition;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class DefaultQueryServiceTest {

    private static final String JSON_VALUE = "{\"test\":\"test\"}";

    @Test
    public void testGetValue() throws Exception {
        Map<String, Value> values = new HashMap<String, Value>();
        values.put("test1", new Value(JSON_VALUE.getBytes()));

        Node node = createMock(Node.class);
        Router router = createMock(Router.class);

        router.routeToNodeFor("bucket", "test1");
        expectLastCall().andReturn(node).once();
        node.send(EasyMock.<GetValueCommand>anyObject());
        expectLastCall().andReturn(values).once();

        replay(node, router);

        DefaultQueryService service = new DefaultQueryService(router);
        assertEquals(JSON_VALUE, new String(service.getValue("bucket", "test1").getBytes()));

        verify(node, router);
    }

    @Test
    public void testGetAllValues() throws Exception {
        Map<String, Value> keys = new HashMap<String, Value>();
        keys.put("test", null);
        Map<String, Value> values = new HashMap<String, Value>();
        values.put("test", new Value(JSON_VALUE.getBytes()));

        Node localNode = createMock(Node.class);
        Node remoteNode = createMock(Node.class);
        Router router = createMock(Router.class);
        Map<Node, Set<String>> nodeToKeys = new HashMap<Node, Set<String>>();
        nodeToKeys.put(remoteNode, new HashSet<String>(Arrays.asList("test")));

        router.getLocalNode();
        expectLastCall().andReturn(localNode).once();
        router.routeToNodesFor("bucket", new HashSet<String>(Arrays.asList("test")));
        expectLastCall().andReturn(nodeToKeys).once();
        localNode.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(keys).once();
        remoteNode.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values).once();

        replay(localNode, remoteNode, router);

        DefaultQueryService service = new DefaultQueryService(router);
        Map<String, Value> result = service.getAllValues("bucket");
        assertEquals(1, result.size());
        assertEquals(JSON_VALUE, new String(result.values().toArray(new Value[1])[0].getBytes()));

        verify(localNode, remoteNode, router);
    }

    @Test
    public void testDoRangeQueryWithNoPredicate() throws Exception {
        Comparator<String> stringComparator = new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };

        Map<String, Value> keys = new HashMap<String, Value>();
        keys.put("test1", null);
        keys.put("test2", null);
        Map<String, Value> values = new HashMap<String, Value>();
        values.put("test1", new Value(JSON_VALUE.getBytes()));
        values.put("test2", new Value(JSON_VALUE.getBytes()));

        Node localNode = createMock(Node.class);
        Node remoteNode = createMock(Node.class);
        Router router = createMock(Router.class);
        Map<Node, Set<String>> nodeToKeys = new HashMap<Node, Set<String>>();
        nodeToKeys.put(remoteNode, new HashSet<String>(Arrays.asList("test1", "test2")));

        router.getLocalNode();
        expectLastCall().andReturn(localNode).once();
        router.routeToNodesFor("bucket", new HashSet<String>(Arrays.asList("test1", "test2")));
        expectLastCall().andReturn(nodeToKeys).once();
        localNode.send(EasyMock.<DoRangeQueryCommand>anyObject());
        expectLastCall().andReturn(keys).once();
        remoteNode.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values).once();

        replay(localNode, remoteNode, router);

        Map<String, Comparator<String>> comparators = new HashMap<String, Comparator<String>>();
        comparators.put("order", stringComparator);

        DefaultQueryService service = new DefaultQueryService(router);
        service.setComparators(comparators);

        Map<String, Value> result = service.doRangeQuery("bucket", new Range("test1", "test2", "order"), new Predicate(), 0);
        assertEquals(2, result.size());
        assertEquals("test1", result.keySet().toArray()[0]);
        assertEquals("test2", result.keySet().toArray()[1]);

        verify(localNode, remoteNode, router);
    }

    @Test
    public void testDoRangeQueryWithPredicate() throws Exception {
        Comparator<String> stringComparator = new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };

        Condition trueCondition = new Condition() {

            @Override
            public boolean isSatisfied(Map<String, Object> value, String expression) {
                return true;
            }
        };

        Map<String, Value> keys = new HashMap<String, Value>();
        keys.put("test1", null);
        keys.put("test2", null);
        Map<String, Value> values = new HashMap<String, Value>();
        values.put("test1", new Value(JSON_VALUE.getBytes()));
        values.put("test2", new Value(JSON_VALUE.getBytes()));

        Node localNode = createMock(Node.class);
        Node remoteNode = createMock(Node.class);
        Router router = createMock(Router.class);
        Map<Node, Set<String>> nodeToKeys = new HashMap<Node, Set<String>>();
        nodeToKeys.put(remoteNode, new HashSet<String>(Arrays.asList("test1", "test2")));

        router.getLocalNode();
        expectLastCall().andReturn(localNode).once();
        router.routeToNodesFor("bucket", new HashSet<String>(Arrays.asList("test1", "test2")));
        expectLastCall().andReturn(nodeToKeys).once();
        localNode.send(EasyMock.<DoRangeQueryCommand>anyObject());
        expectLastCall().andReturn(keys).once();
        remoteNode.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values).once();

        replay(localNode, remoteNode, router);

        Map<String, Comparator<String>> comparators = new HashMap<String, Comparator<String>>();
        comparators.put("order", stringComparator);
        Map<String, Condition> conditions = new HashMap<String, Condition>();
        conditions.put("test", trueCondition);

        DefaultQueryService service = new DefaultQueryService(router);
        service.setComparators(comparators);
        service.setConditions(conditions);

        Map<String, Value> result = service.doRangeQuery("bucket", new Range("test1", "test2", "order"), new Predicate("test:true"), 0);
        assertEquals(2, result.size());
        assertEquals("test1", result.keySet().toArray()[0]);
        assertEquals("test2", result.keySet().toArray()[1]);

        verify(localNode, remoteNode, router);
    }

    @Test(expected = QueryOperationException.class)
    public void testDoRangeQueryWithPredicateFailsDueToNoConditionFound() throws Exception {
        Comparator<String> stringComparator = new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };

        Condition trueCondition = new Condition() {

            @Override
            public boolean isSatisfied(Map<String, Object> value, String expression) {
                return true;
            }
        };

        Map<String, Value> keys = new HashMap<String, Value>();
        keys.put("test1", null);
        keys.put("test2", null);
        Map<String, Value> values = new HashMap<String, Value>();
        values.put("test1", new Value(JSON_VALUE.getBytes()));
        values.put("test2", new Value(JSON_VALUE.getBytes()));

        Node localNode = createMock(Node.class);
        Node remoteNode = createMock(Node.class);
        Router router = createMock(Router.class);
        Map<Node, Set<String>> nodeToKeys = new HashMap<Node, Set<String>>();
        nodeToKeys.put(remoteNode, new HashSet<String>(Arrays.asList("test1", "test2")));

        router.getLocalNode();
        expectLastCall().andReturn(localNode).once();
        router.routeToNodesFor("bucket", new HashSet<String>(Arrays.asList("test1", "test2")));
        expectLastCall().andReturn(nodeToKeys).once();
        localNode.send(EasyMock.<DoRangeQueryCommand>anyObject());
        expectLastCall().andReturn(keys).once();
        remoteNode.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values).once();

        replay(localNode, remoteNode, router);

        Map<String, Comparator<String>> comparators = new HashMap<String, Comparator<String>>();
        comparators.put("order", stringComparator);
        Map<String, Condition> conditions = new HashMap<String, Condition>();
        conditions.put("true", trueCondition);

        DefaultQueryService service = new DefaultQueryService(router);
        service.setComparators(comparators);
        service.setConditions(conditions);

        service.doRangeQuery("bucket", new Range("test1", "test2", "order"), new Predicate("notfound:true"), 0);
    }

    @Test
    public void testDoPredicateQuery() throws Exception {
        Condition trueCondition = new Condition() {

            @Override
            public boolean isSatisfied(Map<String, Object> value, String expression) {
                return true;
            }
        };

        Map<String, Value> keys = new HashMap<String, Value>();
        keys.put("test1", null);
        keys.put("test2", null);
        Map<String, Value> values = new HashMap<String, Value>();
        values.put("test1", new Value(JSON_VALUE.getBytes()));
        values.put("test2", new Value(JSON_VALUE.getBytes()));

        Node localNode = createMock(Node.class);
        Node remoteNode = createMock(Node.class);
        Router router = createMock(Router.class);
        Map<Node, Set<String>> nodeToKeys = new HashMap<Node, Set<String>>();
        nodeToKeys.put(remoteNode, new HashSet<String>(Arrays.asList("test1", "test2")));

        router.getLocalNode();
        expectLastCall().andReturn(localNode).once();
        router.routeToNodesFor("bucket", new HashSet<String>(Arrays.asList("test1", "test2")));
        expectLastCall().andReturn(nodeToKeys).once();
        localNode.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(keys).once();
        remoteNode.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values).once();

        replay(localNode, remoteNode, router);

        Map<String, Condition> conditions = new HashMap<String, Condition>();
        conditions.put("test", trueCondition);

        DefaultQueryService service = new DefaultQueryService(router);
        service.setConditions(conditions);

        Map<String, Value> result = service.doPredicateQuery("bucket", new Predicate("test:true"));
        assertEquals(2, result.size());
        assertEquals("test1", result.keySet().toArray()[0]);
        assertEquals("test2", result.keySet().toArray()[1]);

        verify(localNode, remoteNode, router);
    }

    @Test(expected = QueryOperationException.class)
    public void testDoPredicateQueryFailsDueToNoConditionFound() throws Exception {
        Condition trueCondition = new Condition() {

            @Override
            public boolean isSatisfied(Map<String, Object> value, String expression) {
                return true;
            }
        };

        Map<String, Value> keys = new HashMap<String, Value>();
        keys.put("test1", null);
        keys.put("test2", null);
        Map<String, Value> values = new HashMap<String, Value>();
        values.put("test1", new Value(JSON_VALUE.getBytes()));
        values.put("test2", new Value(JSON_VALUE.getBytes()));

        Node localNode = createMock(Node.class);
        Node remoteNode = createMock(Node.class);
        Router router = createMock(Router.class);
        Map<Node, Set<String>> nodeToKeys = new HashMap<Node, Set<String>>();
        nodeToKeys.put(remoteNode, new HashSet<String>(Arrays.asList("test1", "test2")));

        router.getLocalNode();
        expectLastCall().andReturn(localNode).once();
        router.routeToNodesFor("bucket", new HashSet<String>(Arrays.asList("test1", "test2")));
        expectLastCall().andReturn(nodeToKeys).once();
        localNode.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(keys).once();
        remoteNode.send(EasyMock.<GetValuesCommand>anyObject());
        expectLastCall().andReturn(values).once();

        replay(localNode, remoteNode, router);

        Map<String, Condition> conditions = new HashMap<String, Condition>();
        conditions.put("test", trueCondition);

        DefaultQueryService service = new DefaultQueryService(router);
        service.setConditions(conditions);

        service.doPredicateQuery("bucket", new Predicate("notfound:true"));
    }
}
