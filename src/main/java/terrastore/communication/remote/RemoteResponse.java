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
package terrastore.communication.remote;

import java.io.IOException;
import java.io.Serializable;
import org.msgpack.MessagePackable;
import org.msgpack.MessageTypeException;
import org.msgpack.MessageUnpackable;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.common.ErrorMessage;
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sergio Bossa
 */
public class RemoteResponse implements MessagePackable, MessageUnpackable {

    private String correlationId;
    private Object result;
    private ErrorMessage error;

    public RemoteResponse(String correlationId, Object result) {
        this(correlationId, result, null);
    }

    public RemoteResponse(String correlationId, ErrorMessage error) {
        this(correlationId, null, error);
    }

    public RemoteResponse() {
    }

    protected RemoteResponse(String correlationId, Object result, ErrorMessage error) {
        this.correlationId = correlationId;
        this.result = result;
        this.error = error;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public Object getResult() {
        return result;
    }

    public ErrorMessage getError() {
        return error;
    }

    public boolean isOk() {
        return error == null;
    }

    @Override
    public void messagePack(Packer packer) throws IOException {
        MsgPackUtils.packString(packer, correlationId);
        MsgPackUtils.packObject(packer, result);
        MsgPackUtils.packErrorMessage(packer, error);
    }

    @Override
    public void messageUnpack(Unpacker unpacker) throws IOException, MessageTypeException {
        correlationId = MsgPackUtils.unpackString(unpacker);
        result = MsgPackUtils.unpackObject(unpacker);
        error = MsgPackUtils.unpackErrorMessage(unpacker);
    }
}
