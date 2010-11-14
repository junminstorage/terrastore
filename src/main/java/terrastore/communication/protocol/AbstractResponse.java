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
public abstract class AbstractResponse<R> implements Response<R>, MessagePackable, MessageUnpackable {

    private String correlationId;
    private ErrorMessage error;

    public AbstractResponse(String correlationId) {
        this(correlationId, null);
    }

    public AbstractResponse(String correlationId, ErrorMessage error) {
        this.correlationId = correlationId;
        this.error = error;
    }

    public AbstractResponse() {
    }

    public String getCorrelationId() {
        return correlationId;
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
        MsgPackUtils.packErrorMessage(packer, error);
        doSerialize(packer);
    }

    @Override
    public void messageUnpack(Unpacker unpacker) throws IOException, MessageTypeException {
        correlationId = MsgPackUtils.unpackString(unpacker);
        error = MsgPackUtils.unpackErrorMessage(unpacker);
        doDeserialize(unpacker);
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && (obj instanceof Response) && ((Response) obj).getCorrelationId().equals(this.correlationId);
    }

    @Override
    public int hashCode() {
        return correlationId.hashCode();
    }

    @Override
    public String toString() {
        return correlationId;
    }

    protected abstract void doSerialize(Packer packer) throws IOException;

    protected abstract void doDeserialize(Unpacker unpacker) throws IOException, MessageTypeException;
}
