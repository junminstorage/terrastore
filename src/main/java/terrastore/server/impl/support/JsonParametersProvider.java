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
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import terrastore.common.ErrorMessage;
import terrastore.server.Parameters;
import terrastore.util.io.IOUtils;
import terrastore.util.json.JsonUtils;

/**
 * @author Sergio Bossa
 */
@Provider
@Consumes("application/json")
public class JsonParametersProvider implements MessageBodyReader<Parameters> {

    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return type.equals(Parameters.class);
    }

    public Parameters readFrom(Class<Parameters> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream) throws IOException, WebApplicationException {
        try {
            return JsonUtils.readParameters(entityStream);
        } catch (Exception ex) {
            throw new WebApplicationException(Response.status(ErrorMessage.BAD_REQUEST_ERROR_CODE).
                    entity("Error: " + ex.getMessage() + "\n\rInput: " + new String(IOUtils.read(entityStream))).
                    type("application/json").build());
        }
    }

}
