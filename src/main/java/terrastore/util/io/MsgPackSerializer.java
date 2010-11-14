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
package terrastore.util.io;

import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Sergio Bossa
 */
public class MsgPackSerializer<T> implements Serializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(MsgPackSerializer.class);

    @Override
    public byte[] serialize(T object) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        LZFOutputStream stream = new LZFOutputStream(bytes);
        try {
            Packer packer = new Packer(stream);
            packer.packString(object.getClass().getName());
            packer.pack(object);
            stream.flush();
            return bytes.toByteArray();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            try {
                stream.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
    }

    @Override
    public T deserialize(byte[] serialized) {
        return deserialize(new ByteArrayInputStream(serialized));
    }

    @Override
    public T deserialize(InputStream serialized) {
        LZFInputStream stream = null;
        try {
            stream = new LZFInputStream(serialized);
            Unpacker unpacker = new Unpacker(stream);
            String className = unpacker.unpackString();
            return unpacker.unpack((Class<T>) Class.forName(className));
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            try {
                stream.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
    }

}
