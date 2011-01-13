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
package terrastore.store;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import org.msgpack.MessagePackable;
import org.msgpack.MessageTypeException;
import org.msgpack.MessageUnpackable;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sergio Bossa
 */
public class Key implements Comparable<Key>, MessagePackable, MessageUnpackable, Serializable {

    private static final long serialVersionUID = 12345678901L;
    private static final Charset CHARSET = Charset.forName("UTF-8");
    //
    private String key;

    public Key(String key) {
        this.key = key;
    }

    public Key() {
    }

    public byte[] getBytes() {
        return key.getBytes(CHARSET);
    }

    @Override
    public void messagePack(Packer packer) throws IOException {
        MsgPackUtils.packString(packer, key);
    }

    @Override
    public void messageUnpack(Unpacker unpacker) throws IOException, MessageTypeException {
        key = MsgPackUtils.unpackString(unpacker);
    }

    @Override
    public String toString() {
        return key;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof Key) {
            Key other = (Key) obj;
            return this.key.equals(other.key);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public int compareTo(Key other) {
        return this.key.compareTo(other.key);
    }
}
