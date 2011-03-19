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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import terrastore.store.Bucket;
import terrastore.store.Key;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.util.collect.Maps;
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sergio Bossa
 */
public class PutValuesCommand extends AbstractCommand<Set<Key>> {

    private String bucketName;
    private Map<Key, Value> values;

    public PutValuesCommand(String bucketName, Map<Key, Value> values) {
        this.bucketName = bucketName;
        this.values = values;
    }

    public PutValuesCommand() {
    }

    @Override
    public Response<Set<Key>> executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException {
        Map<Node, Set<Key>> nodeToKeys = router.routeToNodesFor(bucketName, values.keySet());
        Set<Key> result = new HashSet<Key>();
        for (Map.Entry<Node, Set<Key>> nodeToKeysEntry : nodeToKeys.entrySet()) {
            Node node = nodeToKeysEntry.getKey();
            Set<Key> nodeKeys = nodeToKeysEntry.getValue();
            PutValuesCommand command = new PutValuesCommand(bucketName, Maps.slice(values, nodeKeys));
            result.addAll(node.<Set<Key>>send(command));
        }
        return new KeysResponse(id, result);
    }

    public Response<Set<Key>> executeOn(Store store) throws StoreOperationException {
        Bucket bucket = store.getOrCreate(bucketName);
        Set<Key> insertedKeys = new HashSet<Key>();
        for (Map.Entry<Key, Value> entry : values.entrySet()) {
            bucket.put(entry.getKey(), entry.getValue());
            insertedKeys.add(entry.getKey());
        }
        return new KeysResponse(id, insertedKeys);
    }

    @Override
    protected void doSerialize(Packer packer) throws IOException {
        MsgPackUtils.packString(packer, bucketName);
        MsgPackUtils.packValues(packer, values);
    }

    @Override
    protected void doDeserialize(Unpacker unpacker) throws IOException, MessageTypeException {
        bucketName = MsgPackUtils.unpackString(unpacker);
        values = MsgPackUtils.unpackValues(unpacker);
    }
}
