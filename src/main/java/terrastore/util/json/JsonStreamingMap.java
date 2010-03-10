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
import terrastore.store.types.JsonValue;

/**
 * @author Sergio Bossa
 */
public class JsonStreamingMap extends AbstractMap<String, Object> {

    private final JsonValue json;

    public JsonStreamingMap(JsonValue json) {
        this.json = json;
    }

    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        return new JsonStreamingSet();
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
                this.parser = factory.createJsonParser(json.getBytes());
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
                    if (token.equals(JsonToken.START_OBJECT)) {
                        value = extractObject();
                    } else if (token.equals(JsonToken.START_ARRAY)) {
                        value = extractArray();
                    } else if (token.toString().startsWith("VALUE_")) {
                        if (token.equals(JsonToken.VALUE_STRING)) {
                            value = parser.getText();
                        } else if (token.equals(JsonToken.VALUE_NUMBER_FLOAT)) {
                            value = parser.getFloatValue();
                        } else if (token.equals(JsonToken.VALUE_NUMBER_INT)) {
                            value = parser.getIntValue();
                        } else if (token.equals(JsonToken.VALUE_TRUE) || token.equals(JsonToken.VALUE_FALSE)) {
                            value = parser.getBooleanValue();
                        } else {
                            value = null;
                        }
                    } else {
                        return endOfData();
                    }
                    return new JsonEntry(key, value);
                } catch (Exception ex) {
                    return endOfData();
                }
            }

            private Object extractArray() throws IOException {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                JsonGenerator generator = factory.createJsonGenerator(out, JsonEncoding.UTF8);
                generator.copyCurrentStructure(parser);
                generator.close();
                return new JsonStreamingList(new JsonValue(out.toByteArray()));
            }

            private Object extractObject() throws IOException {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                JsonGenerator generator = factory.createJsonGenerator(out, JsonEncoding.UTF8);
                generator.copyCurrentStructure(parser);
                generator.close();
                return new JsonStreamingMap(new JsonValue(out.toByteArray()));
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
