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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.AbstractList;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import terrastore.store.Value;

/**
 * @author Sergio Bossa
 */
public class JsonStreamingList extends AbstractList {

    private final Value json;

    public JsonStreamingList(Value json) {
        this.json = json;
    }

    @Override
    public Object get(int index) {
        try {
            JsonFactory factory = new JsonFactory();
            JsonParser parser = makeParser(factory);
            JsonToken token = advanceParser(parser, index);
            if (token != null) {
                if (token.equals(JsonToken.START_OBJECT)) {
                    return extractObject(factory, parser);
                } else if (token.equals(JsonToken.START_ARRAY)) {
                    return extractArray(factory, parser);
                } else if (token.toString().startsWith("VALUE_")) {
                    if (token.equals(JsonToken.VALUE_STRING)) {
                        return parser.getText();
                    } else if (token.equals(JsonToken.VALUE_NUMBER_FLOAT)) {
                        return parser.getFloatValue();
                    } else if (token.equals(JsonToken.VALUE_NUMBER_INT)) {
                        return parser.getLongValue();
                    } else if (token.equals(JsonToken.VALUE_TRUE) || token.equals(JsonToken.VALUE_FALSE)) {
                        return parser.getBooleanValue();
                    } else {
                        return null;
                    }
                } else {
                    throw new IndexOutOfBoundsException("Out of bounds: " + index);
                }
            } else {
                throw new IndexOutOfBoundsException("Out of bounds: " + index);
            }
        } catch (IndexOutOfBoundsException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    @Override
    public int size() {
        try {
            JsonFactory factory = new JsonFactory();
            JsonParser parser = makeParser(factory);
            return calculateSize(parser);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private JsonParser makeParser(JsonFactory factory) throws IOException {
        JsonParser parser = factory.createJsonParser(json.getInputStream());
        parser.nextToken();
        return parser;
    }

    private JsonToken advanceParser(JsonParser parser, int index) throws IOException {
        JsonToken token = parser.nextValue();
        for (int i = 0; i < index && token != null; i++) {
            if (token.equals(JsonToken.START_OBJECT) || token.equals(JsonToken.START_ARRAY)) {
                parser.skipChildren();
                token = parser.nextValue();
            } else if (token.toString().startsWith("VALUE_")) {
                token = parser.nextValue();
            } else {
                token = null;
            }
        }
        return token;
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

    private int calculateSize(JsonParser parser) throws IOException {
        int size = 0;
        JsonToken token = parser.nextValue();
        while (true) {
            if (token.equals(JsonToken.START_OBJECT) || token.equals(JsonToken.START_ARRAY)) {
                parser.skipChildren();
            } else if (token.equals(JsonToken.END_ARRAY)) {
                break;
            }
            size++;
            token = parser.nextValue();
        }
        return size;
    }
}
