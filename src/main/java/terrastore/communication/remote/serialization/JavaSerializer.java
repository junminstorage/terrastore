/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.communication.remote.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.modules.annotations.InstrumentedClass;

/**
 * @author Sergio Bossa
 */
@InstrumentedClass
public class JavaSerializer<T> implements Serializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JavaSerializer.class);

    public byte[] serialize(T object) {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = null;
        try {
            objectStream = new ObjectOutputStream(byteStream);
            objectStream.writeObject(object);
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new IllegalStateException(ex.getMessage(), ex);
        } finally {
            try {
                objectStream.close();
                byteStream.close();
            } catch (Exception ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
            }
        }
        try {
            return byteStream.toByteArray();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    public T deserialize(byte[] serialized) {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(serialized);
        return deserialize(byteStream);
    }

    public T deserialize(InputStream byteStream) {
        ObjectInputStream objectStream = null;
        try {
            objectStream = new ObjectInputStream(byteStream);
            T result = (T) objectStream.readObject();
            return result;
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new IllegalStateException(ex.getMessage(), ex);
        } finally {
            try {
                objectStream.close();
                byteStream.close();
            } catch (Exception ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
            }
        }
    }
}
