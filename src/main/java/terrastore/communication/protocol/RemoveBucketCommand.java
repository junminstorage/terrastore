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
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sergio Bossa
 */
public class RemoveBucketCommand extends AbstractCommand {

    private String bucketName;

    public RemoveBucketCommand(String bucketName) {
        this.bucketName = bucketName;
    }

    public RemoveBucketCommand() {
    }

    @Override
    public NullResponse executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException {
        Node node = router.routeToLocalNode();
        node.send(this);
        return new NullResponse(id);
    }

    public NullResponse executeOn(Store store) throws StoreOperationException {
        store.remove(bucketName);
        return new NullResponse(id);
    }

    @Override
    protected void doSerialize(Packer packer) throws IOException {
        MsgPackUtils.packString(packer, bucketName);
    }

    @Override
    protected void doDeserialize(Unpacker unpacker) throws IOException, MessageTypeException {
        bucketName = MsgPackUtils.unpackString(unpacker);
    }
}
