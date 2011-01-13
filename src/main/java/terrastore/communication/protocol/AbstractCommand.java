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
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sergio Bossa
 */
public abstract class AbstractCommand<R> implements Command, MessagePackable, MessageUnpackable {

    protected String id = "NULL";

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public void messagePack(Packer packer) throws IOException {
        if (id == null) {
            throw new IOException("Trying to serialize a command with no id!");
        } else {
            MsgPackUtils.packString(packer, id);
            doSerialize(packer);
        }
    }

    @Override
    public void messageUnpack(Unpacker unpacker) throws IOException, MessageTypeException {
        id = MsgPackUtils.unpackString(unpacker);
        doDeserialize(unpacker);

    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && (obj instanceof Command) && ((Command) obj).getId().equals(this.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + ":" + id;
    }

    protected abstract void doSerialize(Packer packer) throws IOException;

    protected abstract void doDeserialize(Unpacker unpacker) throws IOException, MessageTypeException;

}
