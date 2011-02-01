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
package terrastore.server.impl.support;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.store.Value;
import terrastore.util.io.IOUtils;

/**
 * @author Sergio Bossa
 */
@Provider
@Consumes("application/json")
@Produces("application/json")
public class JsonValueProvider implements MessageBodyReader<Value>, MessageBodyWriter<Value> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonValueProvider.class);

    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return type.equals(Value.class);
    }

    public Value readFrom(Class<Value> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream) throws IOException, WebApplicationException {
        byte[] bytes = null;
        try {
            bytes = IOUtils.read(entityStream);
            Value result = new Value(bytes);
            return result;
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new WebApplicationException(Response
                    .status(ErrorMessage.BAD_REQUEST_ERROR_CODE)
                    .entity(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Bad Json value: " + new String(bytes)))
                    .build());
        }
    }

    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Value.class.isAssignableFrom(type);
    }

    public void writeTo(Value value, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        IOUtils.read(value.getInputStream(), entityStream);
    }

    public long getSize(Value value, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

}
