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
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sven Johansson
 */
public class RemoveValuesCommand extends AbstractCommand<Map<Key, Value>> {

    private String bucketName;
    private Set<Key> keys;
    private boolean conditional;
    private Predicate predicate;

    public RemoveValuesCommand(String bucketName, Set<Key> keys) {
        this.bucketName = bucketName;
        this.keys = keys;
        this.conditional = false;
        this.predicate = null;
    }

    public RemoveValuesCommand(String bucketName, Set<Key> keys, Predicate predicate) {
        this.bucketName = bucketName;
        this.keys = keys;
        this.conditional = true;
        this.predicate = predicate;
    }

    public RemoveValuesCommand(RemoveValuesCommand command, Set<Key> keys) {
        this.bucketName = command.bucketName;
        this.conditional = command.conditional;
        this.predicate = command.predicate;
        this.keys = keys;
    }

    public RemoveValuesCommand() {
    }

    @Override
    public Response<Set<Key>> executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException {
        Map<Node, Set<Key>> nodeToKeys = router.routeToNodesFor(bucketName, keys);
        Set<Key> result = new HashSet<Key>();
        for (Map.Entry<Node, Set<Key>> nodeToKeysEntry : nodeToKeys.entrySet()) {
            Node node = nodeToKeysEntry.getKey();
            Set<Key> nodeKeys = nodeToKeysEntry.getValue();
            RemoveValuesCommand command = new RemoveValuesCommand(this, nodeKeys);
            result.addAll(node.<Set<Key>>send(command));
        }
        return new KeysResponse(id, result);
    }

    @Override
    public Response<Set<Key>> executeOn(Store store) throws StoreOperationException {
        Bucket bucket = store.get(bucketName);
        Set<Key> result = new HashSet<Key>();
        if (bucket != null) {
            if (!conditional) {
                for (Key key : keys) {
                    bucket.remove(key);
                    result.add(key);
                }
            } else {
                for (Key key : keys) {
                    boolean removedValue = bucket.conditionalRemove(key, predicate);
                    if (removedValue) {
                        result.add(key);
                    }
                }
            }
        }
        return new KeysResponse(id, result);
    }

    @Override
    protected void doDeserialize(Unpacker unpacker) throws IOException, MessageTypeException {
        bucketName = MsgPackUtils.unpackString(unpacker);
        keys = MsgPackUtils.unpackKeys(unpacker);
        conditional = MsgPackUtils.unpackBoolean(unpacker);
        predicate = MsgPackUtils.unpackPredicate(unpacker);
    }

    @Override
    protected void doSerialize(Packer packer) throws IOException {
        MsgPackUtils.packString(packer, bucketName);
        MsgPackUtils.packKeys(packer, keys);
        MsgPackUtils.packBoolean(packer, conditional);
        MsgPackUtils.packPredicate(packer, predicate);
    }

}
