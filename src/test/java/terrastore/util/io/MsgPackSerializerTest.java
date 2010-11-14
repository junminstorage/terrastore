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

import java.io.IOException;
import org.junit.Test;
import org.msgpack.MessagePackable;
import org.msgpack.MessageTypeException;
import org.msgpack.MessageUnpackable;
import static org.junit.Assert.*;
import org.msgpack.Packer;
import org.msgpack.Unpacker;

/**
 * @author Sergio Bossa
 */
public class MsgPackSerializerTest {

    @Test
    public void testSerializeDeserialize() throws IOException, ClassNotFoundException {
        TestObject obj = new TestObject("test");
        //
        MsgPackSerializer<TestObject> serializer = new MsgPackSerializer<TestObject>();
        //
        byte[] serialized = serializer.serialize(obj);
        TestObject deserialized = serializer.deserialize(serialized);
        assertNotNull(deserialized);
        assertEquals(obj, deserialized);
    }

    public static class TestObject implements MessagePackable, MessageUnpackable {

        private String data;

        public TestObject(String data) {
            this.data = data;
        }

        public TestObject() {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof TestObject) && ((TestObject) obj).data.equals(this.data);
        }

        @Override
        public void messagePack(Packer packer) throws IOException {
            MsgPackUtils.packString(packer, data);
        }

        @Override
        public void messageUnpack(Unpacker unpckr) throws IOException, MessageTypeException {
            data = MsgPackUtils.unpackString(unpckr);
        }
    }
}
