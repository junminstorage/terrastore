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
import org.msgpack.MessageTypeException;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.common.ErrorMessage;
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
 * @author Sergio Bossa
 */
public class GetValueCommand extends AbstractCommand<Value> {

    private String bucketName;
    private Key key;
    private boolean conditional;
    private Predicate predicate;

    public GetValueCommand(String bucketName, Key key) {
        this.bucketName = bucketName;
        this.key = key;
        this.conditional = false;
        this.predicate = null;
    }

    public GetValueCommand(String bucketName, Key key, Predicate predicate) {
        this.bucketName = bucketName;
        this.key = key;
        this.conditional = true;
        this.predicate = predicate;
    }

    public GetValueCommand() {
    }

    @Override
    public Response<Value> executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException {
        Node node = router.routeToNodeFor(bucketName, key);
        return new ValueResponse(id, node.<Value>send(this));
    }

    public Response<Value> executeOn(Store store) throws StoreOperationException {
        Bucket bucket = store.get(bucketName);
        if (bucket != null) {
            if (conditional) {
                Value value = bucket.conditionalGet(key, predicate);
                if (value != null) {
                    return new ValueResponse(id, value);
                } else {
                    throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE,
                            "Unsatisfied condition: " + predicate.getConditionType() + ":" + predicate.getConditionExpression() + " for key: " + key));
                }
            } else {
                return new ValueResponse(id, bucket.get(key));
            }
        } else {
            // Deal with non existent bucket as if it were a non existent key:
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE, "Key not found: " + key));
        }
    }

    @Override
    protected void doSerialize(Packer packer) throws IOException {
        MsgPackUtils.packString(packer, bucketName);
        MsgPackUtils.packKey(packer, key);
        MsgPackUtils.packBoolean(packer, conditional);
        MsgPackUtils.packPredicate(packer, predicate);
    }

    @Override
    protected void doDeserialize(Unpacker unpacker) throws IOException, MessageTypeException {
        bucketName = MsgPackUtils.unpackString(unpacker);
        key = MsgPackUtils.unpackKey(unpacker);
        conditional = MsgPackUtils.unpackBoolean(unpacker);
        predicate = MsgPackUtils.unpackPredicate(unpacker);
    }

}
