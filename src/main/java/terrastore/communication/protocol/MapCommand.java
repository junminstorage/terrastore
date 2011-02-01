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
package terrastore.communication.protocol;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.msgpack.MessageTypeException;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Key;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.features.Mapper;
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sergio Bossa
 */
public class MapCommand extends AbstractCommand<Map<String, Object>> {

    private String bucketName;
    private Set<Key> keys;
    private Mapper mapper;

    public MapCommand(MapCommand command, Set<Key> keys) {
        this.bucketName = command.bucketName;
        this.mapper = command.mapper;
        this.keys = keys;
    }

    public MapCommand(String bucketName, Set<Key> keys, Mapper mapper) {
        this.bucketName = bucketName;
        this.keys = keys;
        this.mapper = mapper;
    }

    public MapCommand() {
    }

    @Override
    public Response<Map<String, Object>> executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException {
        Map<Node, Set<Key>> nodeToKeys = router.routeToNodesFor(bucketName, keys);
        Map<String, Object> result = new HashMap<String, Object>();
        for (Map.Entry<Node, Set<Key>> nodeToKeysEntry : nodeToKeys.entrySet()) {
            Node node = nodeToKeysEntry.getKey();
            Set<Key> nodeKeys = nodeToKeysEntry.getValue();
            MapCommand command = new MapCommand(this, nodeKeys);
            result.putAll(node.<Map<String, Object>>send(command));
        }
        return new GenericMapResponse(id, result);
    }

    public Response<Map<String, Object>> executeOn(Store store) throws StoreOperationException {
        return new GenericMapResponse(id, store.map(bucketName, keys, mapper));
    }

    @Override
    protected void doSerialize(Packer packer) throws IOException {
        MsgPackUtils.packString(packer, bucketName);
        MsgPackUtils.packKeys(packer, keys);
        MsgPackUtils.packMapper(packer, mapper);
    }

    @Override
    protected void doDeserialize(Unpacker unpacker) throws IOException, MessageTypeException {
        bucketName = MsgPackUtils.unpackString(unpacker);
        keys = MsgPackUtils.unpackKeys(unpacker);
        mapper = MsgPackUtils.unpackMapper(unpacker);
    }

}
