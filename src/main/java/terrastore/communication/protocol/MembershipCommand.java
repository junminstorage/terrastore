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
package terrastore.communication.protocol;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.msgpack.MessageTypeException;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.cluster.ensemble.impl.View;
import terrastore.communication.CommunicationException;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;

/**
 * @author Sergio Bossa
 */
public class MembershipCommand extends AbstractCommand<View> {

    @Override
    public View executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException {
        Cluster localCluster = getLocalCluster(router);
        Set<Node> nodes = router.clusterRoute(localCluster);
        Set<View.Member> viewMembers = new HashSet<View.Member>();
        for (Node node : nodes) {
            viewMembers.add(new View.Member(node.getConfiguration()));
        }
        return new View(localCluster.getName(), viewMembers);
    }

    @Override
    public View executeOn(Store store) throws StoreOperationException {
        throw new UnsupportedOperationException("MembershipCommand cannot be executed on a Store!");
    }

    private Cluster getLocalCluster(Router router) {
        Cluster locaCluster = null;
        for (Cluster cluster : router.getClusters()) {
            if (cluster.isLocal()) {
                locaCluster = cluster;
                break;
            }
        }
        return locaCluster;
    }

    @Override
    protected void doSerialize(Packer packer) throws IOException {
    }

    @Override
    protected void doDeserialize(Unpacker unpacker) throws IOException, MessageTypeException {
    }
}
