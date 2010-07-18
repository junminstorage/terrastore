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
package terrastore.store.impl;

import org.junit.Test;
import terrastore.communication.Node;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Bucket;
import terrastore.store.Key;
import static org.easymock.classextension.EasyMock.*;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class RoutingBasedFlushConditionTest {

    @Test
    public void testIsSatisfied() throws MissingRouteException {
        Router router = createMock(Router.class);
        Node local = createMock(Node.class);
        Node other = createMock(Node.class);
        Bucket bucket = createMock(Bucket.class);

        router.routeToLocalNode();
        expectLastCall().andReturn(local).once();
        router.routeToNodeFor("bucket", new Key("key"));
        expectLastCall().andReturn(other).once();
        bucket.getName();
        expectLastCall().andReturn("bucket").once();

        replay(router, bucket, local, other);

        RoutingBasedFlushCondition condition = new RoutingBasedFlushCondition(router);
        assertTrue(condition.isSatisfied(bucket, new Key("key")));

        verify(router, bucket, local, other);
    }

    @Test
    public void testIsNotSatisfied() throws MissingRouteException {
        Router router = createMock(Router.class);
        Node local = createMock(Node.class);
        Bucket bucket = createMock(Bucket.class);

        router.routeToLocalNode();
        expectLastCall().andReturn(local).once();
        router.routeToNodeFor("bucket", new Key("key"));
        expectLastCall().andReturn(local).once();
        bucket.getName();
        expectLastCall().andReturn("bucket").once();

        replay(router, bucket, local);

        RoutingBasedFlushCondition condition = new RoutingBasedFlushCondition(router);
        assertFalse(condition.isSatisfied(bucket, new Key("key")));

        verify(router, bucket, local);
    }
}