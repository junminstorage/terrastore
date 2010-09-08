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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.store.Value;
import terrastore.store.types.JsonValue;

/**
 * @author Sergio Bossa
 */
public class MagicSerializer implements Serializer<Value> {

    private static final Logger LOG = LoggerFactory.getLogger(MagicSerializer.class);
    //
    private static final byte JSON_MAGIC = 0x1;
    private static final Map<Class, Byte> MAGICS = new HashMap<Class, Byte>();
    private static final Map<Byte, ValueFactory> FACTORIES = new HashMap<Byte, ValueFactory>();

    {
        MAGICS.put(JsonValue.class, JSON_MAGIC);
        FACTORIES.put(JSON_MAGIC, new JsonValueFactory());
    }

    @Override
    public final byte[] serialize(Value value) {
        byte[] source = value.getBytes();
        byte[] serialized = new byte[source.length + 1];
        System.arraycopy(source, 0, serialized, 1, source.length);
        serialized[0] = MAGICS.get(value.getClass());
        return serialized;
    }

    @Override
    public final Value deserialize(byte[] serialized) {
        ValueFactory factory = FACTORIES.get(serialized[0]);
        if (factory != null) {
            byte[] destination = new byte[serialized.length - 1];
            System.arraycopy(serialized, 1, destination, 0, destination.length);
            return factory.makeValue(destination);
        } else {
            return null;
        }
    }

    public Value deserialize(InputStream byteStream) {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            int read = 0;
            while ((read = byteStream.read()) > -1) {
                output.write(read);
            }
            return deserialize(output.toByteArray());
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    private static interface ValueFactory {

        public Value makeValue(byte[] bytes);
    }

    private static class JsonValueFactory implements ValueFactory {

        @Override
        public Value makeValue(byte[] bytes) {
            return new JsonValue(bytes);
        }
    }
}
