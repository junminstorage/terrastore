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
package terrastore.util.io;

import terrastore.util.io.JavaSerializer;
import terrastore.util.io.Serializer;
import org.junit.Test;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.features.Update;
import terrastore.store.operators.Condition;
import terrastore.store.operators.Function;
import static org.junit.Assert.*;

public class JavaSerializerTest {

    private static final String VALUE = "test";

    @Test
    public void testSerializeDeserialize() {
        Value value = new TestValue(VALUE);
        Serializer<Value> serializer = new JavaSerializer();
        byte[] serialized = serializer.serialize(value);
        Value deserialized = serializer.deserialize(serialized);
        assertArrayEquals(value.getBytes(), deserialized.getBytes());
    }

    private static class TestValue implements Value {

        private final String content;

        public TestValue(String content) {
            this.content = content;
        }

        @Override
        public byte[] getBytes() {
            try {
                return content.getBytes("UTF-8");
            } catch (Exception ex) {
                return null;
            }
        }

        @Override
        public Value dispatch(Key key, Update update, Function function) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean dispatch(Key key, Predicate predicate, Condition condition) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
