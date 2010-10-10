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
package terrastore.util.json;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.common.ClusterStats;
import terrastore.common.ErrorMessage;
import terrastore.server.Buckets;
import terrastore.server.Parameters;
import terrastore.server.Values;
import terrastore.store.Key;
import terrastore.store.Value;

/**
 * @author Sergio Bossa
 */
public class JsonUtils {

    private static ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static Map<String, Object> toModifiableMap(Value value) {
        try {
            return JSON_MAPPER.readValue(new ByteArrayInputStream(value.getBytes()), Map.class);
        } catch (Exception ex) {
            throw new IllegalStateException("Value should have been already validated!");
        }
    }

    public static Map<String, Object> toUnmodifiableMap(Value value) {
        try {
            return new JsonStreamingMap(value);
        } catch (Exception ex) {
            throw new IllegalStateException("Value should have been already validated!");
        }
    }

    public static Value fromMap(Map<String, Object> value) {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            JSON_MAPPER.writeValue(output, value);
            return new Value(output.toByteArray());
        } catch (Exception ex) {
            throw new IllegalStateException("Value should have been already validated!");
        }
    }

    public static void validate(Value value) throws IOException {
        JsonParser parser = new JsonFactory().createJsonParser(value.getBytes());
        JsonToken currentToken = parser.nextToken();
        if (currentToken.equals(JsonToken.START_ARRAY)) {
            validateArray(parser);
        } else if (currentToken.equals(JsonToken.START_OBJECT)) {
            validateObject(parser);
        } else {
            throw new IOException("Expected object/array start, found: " + currentToken.toString());
        }
    }

    public static void write(ClusterStats clusterStats, OutputStream stream) throws IOException {
        JSON_MAPPER.writeValue(stream, clusterStats);
    }

    public static void write(ErrorMessage errorMessage, OutputStream stream) throws IOException {
        JSON_MAPPER.writeValue(stream, errorMessage);
    }

    public static void write(Buckets buckets, OutputStream stream) throws IOException {
        JSON_MAPPER.writeValue(stream, buckets);
    }

    public static void write(Values values, OutputStream stream) throws IOException {
        JsonGenerator jsonGenerator = new JsonFactory().createJsonGenerator(stream, JsonEncoding.UTF8);
        Set<Map.Entry<Key, Value>> entries = values.entrySet();
        jsonGenerator.writeStartObject();
        for (Map.Entry<Key, Value> entry : entries) {
            Key key = entry.getKey();
            Value value = entry.getValue();
            jsonGenerator.writeFieldName(key.toString());
            jsonGenerator.writeRawValue(value.toString());
        }
        jsonGenerator.writeEndObject();
        jsonGenerator.close();
    }

    public static Parameters readParameters(InputStream stream) throws IOException {
        return JSON_MAPPER.readValue(stream, Parameters.class);
    }

    public static EnsembleConfiguration readEnsembleConfiguration(InputStream stream) throws IOException {
        return JSON_MAPPER.readValue(stream, EnsembleConfiguration.class);
    }

    private static void validateObject(JsonParser parser) throws IOException {
        JsonToken currentToken = parser.nextValue();
        while (currentToken != null && !currentToken.equals(JsonToken.END_OBJECT)) {
            if (currentToken.toString().startsWith("VALUE_")) {
                currentToken = parser.nextValue();
            } else if (currentToken.equals(JsonToken.START_ARRAY)) {
                validateArray(parser);
                currentToken = parser.nextValue();
            } else if (currentToken.equals(JsonToken.START_OBJECT)) {
                validateObject(parser);
                currentToken = parser.nextValue();
            } else {
                throw new IOException("Expected object/array start, found: " + currentToken.toString());
            }
        }
    }

    private static void validateArray(JsonParser parser) throws IOException {
        JsonToken currentToken = parser.nextValue();
        while (currentToken != null && !currentToken.equals(JsonToken.END_ARRAY)) {
            if (currentToken.toString().startsWith("VALUE_")) {
                currentToken = parser.nextValue();
            } else if (currentToken.equals(JsonToken.START_ARRAY)) {
                validateArray(parser);
                currentToken = parser.nextValue();
            } else if (currentToken.equals(JsonToken.START_OBJECT)) {
                validateObject(parser);
                currentToken = parser.nextValue();
            } else {
                throw new IOException("Expected object/array start, found: " + currentToken.toString());
            }
        }
    }
}
