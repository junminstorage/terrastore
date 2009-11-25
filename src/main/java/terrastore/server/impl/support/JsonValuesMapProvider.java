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
package terrastore.server.impl.support;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import terrastore.server.Values;
import terrastore.store.Value;

/**
 * @author Sergio Bossa
 */
@Provider
@Produces("application/json")
public class JsonValuesMapProvider implements MessageBodyWriter<Values> {

    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Values.class.isAssignableFrom(type);
    }

    public void writeTo(Values values, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        JsonGenerator jsonGenerator = new JsonFactory().createJsonGenerator(entityStream, JsonEncoding.UTF8);
        Set<Map.Entry<String, Value>> entries = values.entrySet();
        jsonGenerator.writeStartObject();
        for (Map.Entry<String, Value> entry : entries) {
            String key = entry.getKey();
            Value value = entry.getValue();
            jsonGenerator.writeFieldName(key);
            jsonGenerator.writeRawValue(new String(value.getBytes(), "UTF-8"));
        }
        jsonGenerator.writeEndObject();
        jsonGenerator.close();
    }

    public long getSize(Values values, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }
}
