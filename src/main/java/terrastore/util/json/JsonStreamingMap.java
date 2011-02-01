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
package terrastore.util.json;

import com.google.common.collect.AbstractIterator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import terrastore.store.Value;

/**
 * @author Sergio Bossa
 */
public class JsonStreamingMap extends AbstractMap<String, Object> {

    private final Value json;

    public JsonStreamingMap(Value json) {
        this.json = json;
    }

    @Override
    public final Object get(Object candidate) {
        try {
            JsonFactory factory = new JsonFactory();
            JsonParser parser = factory.createJsonParser(json.getInputStream());
            String key = navigateToKey(parser, candidate.toString());
            if (key != null) {
                return getObjectValue(factory, parser);
            } else {
                return null;
            }
        } catch (Exception ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    @Override
    public final boolean containsKey(Object candidate) {
        try {
            JsonFactory factory = new JsonFactory();
            JsonParser parser = factory.createJsonParser(json.getInputStream());
            return navigateToKey(parser, candidate.toString()) != null;
        } catch (Exception ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    @Override
    public final Set<Map.Entry<String, Object>> entrySet() {
        return new JsonStreamingSet();
    }

    private String navigateToKey(JsonParser parser, String candidate) throws IOException {
        JsonToken token = parser.nextToken();
        while (token != null && (!token.equals(JsonToken.FIELD_NAME) || !parser.getCurrentName().equals(candidate))) {
            token = parser.nextToken();
            if (token != null && (token.equals(JsonToken.START_OBJECT) || token.equals(JsonToken.START_ARRAY))) {
                parser.skipChildren();
            }
        }
        return parser.getCurrentName();
    }

    private Object getObjectValue(JsonFactory factory, JsonParser parser) throws IOException {
        JsonToken token = parser.nextToken();
        Object value = null;
        if (token != null) {
            if (token.equals(JsonToken.START_OBJECT)) {
                value = extractObject(factory, parser);
            } else if (token.equals(JsonToken.START_ARRAY)) {
                value = extractArray(factory, parser);
            } else if (token.toString().startsWith("VALUE_")) {
                if (token.equals(JsonToken.VALUE_STRING)) {
                    value = parser.getText();
                } else if (token.equals(JsonToken.VALUE_NUMBER_FLOAT)) {
                    value = parser.getFloatValue();
                } else if (token.equals(JsonToken.VALUE_NUMBER_INT)) {
                    value = parser.getLongValue();
                } else if (token.equals(JsonToken.VALUE_TRUE) || token.equals(JsonToken.VALUE_FALSE)) {
                    value = parser.getBooleanValue();
                } else {
                    value = null;
                }
            } else {
                return null;
            }
            return value;
        } else {
            return null;
        }
    }

    private Object extractArray(JsonFactory factory, JsonParser parser) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonGenerator generator = factory.createJsonGenerator(out, JsonEncoding.UTF8);
        generator.copyCurrentStructure(parser);
        generator.close();
        return new JsonStreamingList(new Value(out.toByteArray()));
    }

    private Object extractObject(JsonFactory factory, JsonParser parser) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonGenerator generator = factory.createJsonGenerator(out, JsonEncoding.UTF8);
        generator.copyCurrentStructure(parser);
        generator.close();
        return new JsonStreamingMap(new Value(out.toByteArray()));
    }

    private class JsonStreamingSet extends AbstractSet<Map.Entry<String, Object>> {

        @Override
        public Iterator<Map.Entry<String, Object>> iterator() {
            try {
                return new JsonStreamingIterator();
            } catch (IOException ex) {
                return new AbstractIterator<Entry<String, Object>>() {

                    @Override
                    protected Entry computeNext() {
                        return endOfData();
                    }
                };
            }
        }

        @Override
        public int size() {
            try {
                int size = 0;
                Iterator it = new JsonStreamingIterator();
                while (it.hasNext()) {
                    it.next();
                    size++;
                }
                return size;
            } catch (Exception ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }

        private class JsonStreamingIterator extends AbstractIterator<Map.Entry<String, Object>> {

            private final JsonFactory factory;
            private final JsonParser parser;

            public JsonStreamingIterator() throws IOException {
                this.factory = new JsonFactory();
                this.parser = factory.createJsonParser(json.getInputStream());
                this.parser.nextToken();
            }

            @Override
            protected Map.Entry<String, Object> computeNext() {
                try {
                    JsonToken token = parser.nextToken();
                    String key = parser.getText();
                    Object value = null;
                    //
                    token = parser.nextToken();
                    if (token != null) {
                        if (token.equals(JsonToken.START_OBJECT)) {
                            value = extractObject(factory, parser);
                        } else if (token.equals(JsonToken.START_ARRAY)) {
                            value = extractArray(factory, parser);
                        } else if (token.toString().startsWith("VALUE_")) {
                            if (token.equals(JsonToken.VALUE_STRING)) {
                                value = parser.getText();
                            } else if (token.equals(JsonToken.VALUE_NUMBER_FLOAT)) {
                                value = parser.getFloatValue();
                            } else if (token.equals(JsonToken.VALUE_NUMBER_INT)) {
                                value = parser.getLongValue();
                            } else if (token.equals(JsonToken.VALUE_TRUE) || token.equals(JsonToken.VALUE_FALSE)) {
                                value = parser.getBooleanValue();
                            } else {
                                value = null;
                            }
                        } else {
                            return endOfData();
                        }
                        return new JsonEntry(key, value);
                    } else {
                        return endOfData();
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex.getMessage(), ex);
                }
            }

            private class JsonEntry implements Map.Entry<String, Object> {

                private String key;
                private Object value;

                public JsonEntry(String key, Object value) {
                    this.key = key;
                    this.value = value;
                }

                @Override
                public String getKey() {
                    return key;
                }

                @Override
                public Object getValue() {
                    return value;
                }

                @Override
                public Object setValue(Object value) {
                    throw new UnsupportedOperationException("This entry is unmodifiable!");
                }

            }
        }
    }
}
