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
import java.util.Set;
import org.msgpack.MessageTypeException;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.util.collect.Sets;

/**
 * @author Sergio Bossa
 */
public class GetBucketsCommand extends AbstractCommand<Set<String>> {

    @Override
    public Set<String> executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException {
        Node node = router.routeToLocalNode();
        return Sets.serializing(node.<Set<String>>send(this));
    }

    public Set<String> executeOn(Store store) throws StoreOperationException {
        Set<String> buckets = store.buckets();
        return Sets.serializing(buckets);
    }

    @Override
    protected void doSerialize(Packer packer) throws IOException {
    }

    @Override
    protected void doDeserialize(Unpacker unpacker) throws IOException, MessageTypeException {
    }

}
