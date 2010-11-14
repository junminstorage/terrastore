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

import org.junit.Test;
import static org.junit.Assert.*;

public class JavaSerializerTest {

    @Test
    public void testSerializeDeserialize() {
        TestObject obj = new TestObject("data");
        JavaSerializer<TestObject> serializer = new JavaSerializer();
        byte[] serialized = serializer.serialize(obj);
        TestObject deserialized = serializer.deserialize(serialized);
        assertEquals(obj, deserialized);
    }

    private static class TestObject {

        private Object data;

        public TestObject(Object data) {
            this.data = data;
        }

        protected TestObject() {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof TestObject) && ((TestObject) obj).data.equals(this.data);
        }
    }
}
