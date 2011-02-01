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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.msgpack.MessageTypeException;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.communication.CommunicationException;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import terrastore.store.features.Reducer;
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sergio Bossa
 */
public class ReduceCommand extends AbstractCommand<Value> {

    private List<Map<String, Object>> values;
    private Reducer reducer;

    public ReduceCommand(List<Map<String, Object>> values, Reducer reducer) {
        this.values = values;
        this.reducer = reducer;
    }

    public ReduceCommand() {
    }

    @Override
    public Response<Value> executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException {
        return new ValueResponse(id, router.routeToLocalNode().<Value>send(this));
    }

    public Response<Value> executeOn(final Store store) throws StoreOperationException {
        return new ValueResponse(id, store.reduce(values, reducer));
    }

    @Override
    protected void doSerialize(Packer packer) throws IOException {
        int size = values.size();
        MsgPackUtils.packInt(packer, size);
        for (Map<String, Object> map : values) {
            MsgPackUtils.packGenericMap(packer, map);
        }
        MsgPackUtils.packReducer(packer, reducer);
    }

    @Override
    protected void doDeserialize(Unpacker unpacker) throws IOException, MessageTypeException {
        int size = MsgPackUtils.unpackInt(unpacker);
        values = new ArrayList<Map<String, Object>>(size);
        for (int i = 0; i < size; i++) {
            values.add(MsgPackUtils.unpackGenericMap(unpacker));
        }
        reducer = MsgPackUtils.unpackReducer(unpacker);
    }

}
