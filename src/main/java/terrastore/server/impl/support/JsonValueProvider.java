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
package terrastore.server.impl.support;

import terrastore.util.json.JsonUtils;
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
import terrastore.util.io.InputReader;
import terrastore.store.Value;
import terrastore.store.types.JsonValue;

/**
 * @author Sergio Bossa
 */
@Provider
@Consumes("application/json")
@Produces("application/json")
public class JsonValueProvider implements MessageBodyReader<Value>, MessageBodyWriter<Value> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonValueProvider.class);
    private final InputReader reader = new InputReader();

    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return type.equals(Value.class);
    }

    public JsonValue readFrom(Class<Value> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream) throws IOException, WebApplicationException {
        byte[] bytes = null;
        try {
            bytes = reader.read(entityStream);
            JsonValue result = new JsonValue(bytes);
            JsonUtils.validate(result);
            return result;
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new WebApplicationException(Response.status(ErrorMessage.BAD_REQUEST_ERROR_CODE).
                    entity(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Bad Json value: " + new String(bytes))).
                    build());
        }
    }

    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Value.class.isAssignableFrom(type);
    }

    public void writeTo(Value value, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        entityStream.write(value.getBytes());
    }

    public long getSize(Value value, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }
}
