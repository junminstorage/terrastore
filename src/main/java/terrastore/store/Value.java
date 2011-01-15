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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import org.msgpack.MessagePackable;
import org.msgpack.MessageTypeException;
import org.msgpack.MessageUnpackable;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.store.features.Mapper;
import terrastore.store.features.Predicate;
import terrastore.store.features.Update;
import terrastore.store.operators.Condition;
import terrastore.store.operators.Function;
import terrastore.util.io.IOUtils;
import terrastore.util.io.MsgPackUtils;
import terrastore.util.json.JsonUtils;

/**
 * Json value object contained by {@link Bucket} instances.
 *
 * @author Sergio Bossa
 */
public class Value implements MessagePackable, MessageUnpackable, Serializable {

    private static final long serialVersionUID = 12345678901L;
    private static final Charset CHARSET = Charset.forName("UTF-8");
    //
    private byte[] bytes;
    private boolean compressed;

    public Value(byte[] bytes) {
        this.bytes = bytes;
        this.compressed = IOUtils.isCompressed(bytes);
    }

    public Value() {
    }

    public final byte[] getBytes() {
        try {
            if (compressed) {
                return IOUtils.readCompressed(new ByteArrayInputStream(bytes));
            } else {
                return bytes;
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    public final byte[] getCompressedBytes() {
        try {
            if (compressed) {
                return bytes;
            } else {
                return IOUtils.readAndCompress(new ByteArrayInputStream(bytes));
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    public final InputStream getInputStream() {
        try {
            if (compressed) {
                return IOUtils.getCompressedInputStream(bytes);
            } else {
                return new ByteArrayInputStream(bytes);
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    public final Value dispatch(Key key, Update update, Function function) {
        return JsonUtils.fromMap(function.apply(key.toString(), JsonUtils.toModifiableMap(this), update.getParameters()));
    }

    public final Map<String, Object> dispatch(Key key, Mapper mapper, Function function) {
        return function.apply(key.toString(), JsonUtils.toUnmodifiableMap(this), mapper.getParameters());
    }

    public final boolean dispatch(Key key, Predicate predicate, Condition condition) {
        return condition.isSatisfied(key.toString(), JsonUtils.toUnmodifiableMap(this), predicate.getConditionExpression());
    }

    @Override
    public void messagePack(Packer packer) throws IOException {
        MsgPackUtils.packBytes(packer, bytes);
        MsgPackUtils.packBoolean(packer, compressed);
    }

    @Override
    public void messageUnpack(Unpacker unpacker) throws IOException, MessageTypeException {
        bytes = MsgPackUtils.unpackBytes(unpacker);
        compressed = MsgPackUtils.unpackBoolean(unpacker);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Value) {
            Value other = (Value) obj;
            return Arrays.equals(other.bytes, this.bytes);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }

    @Override
    public String toString() {
        try {
            if (!compressed) {
                return new String(bytes, CHARSET);
            } else {
                return new String(IOUtils.readCompressed(new ByteArrayInputStream(bytes)), CHARSET);
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

}
