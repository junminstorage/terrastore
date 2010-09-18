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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.easymock.classextension.EasyMock;
import org.junit.Test;
import terrastore.common.ErrorMessage;
import terrastore.communication.Cluster;
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.PutValueCommand;
import terrastore.communication.protocol.RemoveBucketCommand;
import terrastore.communication.protocol.RemoveValueCommand;
import terrastore.communication.protocol.UpdateCommand;
import terrastore.router.Router;
import terrastore.store.Key;
import terrastore.store.features.Predicate;
import terrastore.store.operators.Function;
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

        DefaultUpdateService service = new DefaultUpdateService(router);
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

        DefaultUpdateService service = new DefaultUpdateService(router);
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
            DefaultUpdateService service = new DefaultUpdateService(router);
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
            DefaultUpdateService service = new DefaultUpdateService(router);
            service.removeBucket("bucket");
        } finally {
            verify(cluster1, router);
        }
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

        DefaultUpdateService service = new DefaultUpdateService(router);
        service.putValue("bucket", new Key("test1"), new Value(JSON_VALUE.getBytes()), new Predicate(null));

        verify(node, router);
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

        DefaultUpdateService service = new DefaultUpdateService(router);
        service.removeValue("bucket", new Key("test1"));

        verify(node, router);
    }

    @Test
    public void testUpdateValue() throws Exception {
        Function function = new Function() {

            @Override
            public Map<String, Object> apply(String key, Map<String, Object> value, Map<String, Object> parameters) {
                return value;
            }
        };

        Node node = createMock(Node.class);
        Router router = createMock(Router.class);

        router.routeToNodeFor("bucket", new Key("test1"));
        expectLastCall().andReturn(node).once();
        node.send(EasyMock.<UpdateCommand>anyObject());
        expectLastCall().andReturn(new Value(JSON_VALUE.getBytes())).once();

        replay(node, router);

        Map<String, Function> functions = new HashMap<String, Function>();
        functions.put("update", function);

        DefaultUpdateService service = new DefaultUpdateService(router);
        service.setFunctions(functions);
        assertEquals(new Value(JSON_VALUE.getBytes()), service.updateValue("bucket", new Key("test1"), new Update("update", 1000, new HashMap<String, Object>())));

        verify(node, router);
    }
}
