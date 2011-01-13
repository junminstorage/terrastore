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
import java.util.Map;
import org.msgpack.MessageTypeException;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sergio Bossa
 */
public class GenericMapResponse extends AbstractResponse<Map<String, Object>> {

    private Map<String, Object> result;

    public GenericMapResponse(String correlationId, Map<String, Object> result) {
        super(correlationId);
        this.result = result;
    }

    public GenericMapResponse() {
    }

    @Override
    public Map<String, Object> getResult() {
        return result;
    }

    @Override
    protected void doSerialize(Packer packer) throws IOException {
        MsgPackUtils.packGenericMap(packer, result);
    }

    @Override
    protected void doDeserialize(Unpacker unpacker) throws IOException, MessageTypeException {
        result = MsgPackUtils.unpackGenericMap(unpacker);
    }
}
