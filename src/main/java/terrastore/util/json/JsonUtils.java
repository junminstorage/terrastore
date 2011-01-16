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

import terrastore.store.ValidationException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.common.ClusterStats;
import terrastore.common.ErrorMessage;
import terrastore.server.Buckets;
import terrastore.server.Keys;
import terrastore.server.MapReduceDescriptor;
import terrastore.server.Parameters;
import terrastore.server.Values;
import terrastore.store.Key;
import terrastore.store.Value;

/**
 * @author Sergio Bossa
 */
public class JsonUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static Map<String, Object> toModifiableMap(Value value) {
        try {
            return JSON_MAPPER.readValue(value.getInputStream(), Map.class);
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

    public static void validate(Value value) throws ValidationException {
        try {
            JsonParser parser = new JsonFactory().createJsonParser(value.getBytes());
            JsonToken currentToken = parser.nextToken();
            if (currentToken.equals(JsonToken.START_OBJECT)) {
                validateObject(parser);
            } else if (currentToken.equals(JsonToken.START_ARRAY)) {
                throw new ValidationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Json documents starting with arrays are not supported!"));
            } else {
                throw new ValidationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Bad Json value starting with: " + currentToken.toString()));
            }
        } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new ValidationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Bad Json value: " + ex.getMessage()));
        }
    }

    public static Value merge(Value value, Map<String, Object> updates) throws ValidationException {
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            JsonFactory factory = new JsonFactory();
            JsonGenerator generator = factory.createJsonGenerator(stream, JsonEncoding.UTF8);
            JsonParser parser = factory.createJsonParser(value.getBytes());
            JsonToken currentToken = parser.nextToken();
            generator.setCodec(JSON_MAPPER);
            if (currentToken.equals(JsonToken.START_OBJECT)) {
                mergeObject(parser, generator, updates);
            } else if (currentToken.equals(JsonToken.START_ARRAY)) {
                throw new ValidationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Json documents starting with arrays are not supported!"));
            } else {
                throw new ValidationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Bad Json value starting with: " + currentToken.toString()));
            }
            generator.close();
            return new Value(stream.toByteArray());
        } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new ValidationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Bad Json value: " + ex.getMessage()));
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

    public static void write(Keys keys, OutputStream stream) throws IOException {
        JsonGenerator jsonGenerator = new JsonFactory().createJsonGenerator(stream, JsonEncoding.UTF8);
        jsonGenerator.writeStartArray();
        for (Key key : keys) {
            jsonGenerator.writeString(key.toString());
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.close();
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

    public static MapReduceDescriptor readMapReduceDescriptor(InputStream stream) throws IOException {
        return JSON_MAPPER.readValue(stream, MapReduceDescriptor.class);
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

    private static void mergeObject(JsonParser parser, JsonGenerator generator, Map<String, Object> updates) throws IOException {
        JsonToken currentToken = parser.nextValue();
        generator.writeStartObject();
        while (currentToken != null && !currentToken.equals(JsonToken.END_OBJECT)) {
            String currentField = parser.getCurrentName();
            Object valueToAdd = updates.get('+' + currentField);
            Object valueToRemove = updates.get('-' + currentField);
            Object valueToReplace = updates.get('*' + currentField);
            Object valueToFollow = updates.get(currentField);
            if (valueToAdd != null && valueToAdd instanceof Map && currentToken.equals(JsonToken.START_OBJECT)) {
                addToObject(parser, generator, currentField, (Map) valueToAdd);
            } else if (valueToAdd != null && valueToAdd instanceof List && currentToken.equals(JsonToken.START_ARRAY)) {
                addToArray(parser, generator, currentField, (List) valueToAdd);
            } else if (valueToRemove != null && (currentToken.toString().startsWith("VALUE_") || currentToken.equals(JsonToken.START_OBJECT))) {
                removeValue(parser);
            } else if (valueToRemove != null && valueToRemove instanceof List && currentToken.equals(JsonToken.START_ARRAY)) {
                removeFromArray(parser, generator, currentField, (List) valueToRemove);
            } else if (valueToReplace != null && (currentToken.toString().startsWith("VALUE_") || currentToken.equals(JsonToken.START_ARRAY) || currentToken.
                    equals(JsonToken.START_OBJECT))) {
                replaceValue(generator, currentField, valueToReplace);
            } else if (valueToFollow != null && valueToFollow instanceof Map && currentToken.equals(JsonToken.START_OBJECT)) {
                generator.writeFieldName(currentField);
                mergeObject(parser, generator, (Map) valueToFollow);
            } else {
                generator.writeFieldName(currentField);
                generator.copyCurrentStructure(parser);
            }
            currentToken = parser.skipChildren().nextValue();
        }
        addNewObjectsIfAny(generator, updates);
        generator.writeEndObject();
    }

    private static void addToObject(JsonParser parser, JsonGenerator generator, String field, Map<String, Object> values) throws IOException {
        JsonToken currentToken = parser.nextToken();
        generator.writeFieldName(field);
        generator.writeStartObject();
        while (currentToken != null && !currentToken.equals(JsonToken.END_OBJECT)) {
            generator.copyCurrentStructure(parser);
            currentToken = parser.nextToken();
        }
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            generator.writeFieldName(entry.getKey());
            generator.writeObject(entry.getValue());
        }
        generator.writeEndObject();
    }

    private static void replaceValue(JsonGenerator generator, String field, Object element) throws IOException {
        generator.writeFieldName(field);
        generator.writeObject(element);
    }

    private static void removeValue(JsonParser parser) throws IOException {
        parser.skipChildren();
    }

    private static void addToArray(JsonParser parser, JsonGenerator generator, String field, List<Object> elements) throws IOException {
        JsonToken currentToken = parser.nextValue();
        generator.writeFieldName(field);
        generator.writeStartArray();
        while (currentToken != null && !currentToken.equals(JsonToken.END_ARRAY)) {
            generator.copyCurrentStructure(parser);
            currentToken = parser.nextValue();
        }
        for (Object element : elements) {
            generator.writeObject(element);
        }
        generator.writeEndArray();
    }

    private static void removeFromArray(JsonParser parser, JsonGenerator generator, String field, List<Object> indexes) throws IOException {
        JsonToken currentToken = parser.nextValue();
        int index = 0;
        generator.writeFieldName(field);
        generator.writeStartArray();
        while (currentToken != null && !currentToken.equals(JsonToken.END_ARRAY)) {
            if (!indexes.contains(index) && !indexes.contains(Integer.toString(index))) {
                generator.copyCurrentStructure(parser);
                currentToken = parser.nextValue();
            } else {
                currentToken = parser.skipChildren().nextValue();
            }
            index++;
        }
        generator.writeEndArray();
    }

    private static void addNewObjectsIfAny(JsonGenerator generator, Map<String, Object> updates) throws IOException {
        Object valuesToAdd = updates.get("+");
        if (valuesToAdd != null && valuesToAdd instanceof Map) {
            Map<String, Object> newValues = (Map<String, Object>) valuesToAdd;
            for (Map.Entry<String, Object> entry : newValues.entrySet()) {
                generator.writeFieldName(entry.getKey());
                generator.writeObject(entry.getValue());
            }
        }
    }

}
