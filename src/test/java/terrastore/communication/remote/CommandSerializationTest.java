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
package terrastore.communication.remote;

import terrastore.util.io.*;
import java.io.IOException;
import java.nio.charset.Charset;
import org.junit.Test;
import terrastore.common.ErrorMessage;
import static org.junit.Assert.*;
import terrastore.communication.protocol.GetValueCommand;
import terrastore.communication.protocol.NullResponse;
import terrastore.communication.protocol.ValueResponse;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.store.features.Predicate;

/**
 * @author Sergio Bossa
 */
// FIXME : incomplete!!!!
public class CommandSerializationTest {

    @Test
    public void testGetValueCommand() throws IOException, ClassNotFoundException {
        GetValueCommand command = new GetValueCommand("bucket", new Key("key"), new Predicate("type:expression"));
        command.setId("test");
        //
        MsgPackSerializer<GetValueCommand> serializer = new MsgPackSerializer<GetValueCommand>(false);
        //
        byte[] serialized = serializer.serialize(command);
        GetValueCommand deserialized = serializer.deserialize(serialized);
        assertNotNull(deserialized);
        assertEquals(command, deserialized);
    }

    @Test
    public void testValueResponse() throws IOException, ClassNotFoundException {
        ValueResponse response = new ValueResponse("id", new Value("value".getBytes(Charset.forName("UTF-8"))));
        //
        MsgPackSerializer<ValueResponse> serializer = new MsgPackSerializer<ValueResponse>(false);
        //
        byte[] serialized = serializer.serialize(response);
        ValueResponse deserialized = serializer.deserialize(serialized);
        assertNotNull(deserialized);
        assertEquals(response, deserialized);
    }

    @Test
    public void testNullResponse() throws IOException, ClassNotFoundException {
        NullResponse response = new NullResponse("id");
        //
        MsgPackSerializer<NullResponse> serializer = new MsgPackSerializer<NullResponse>(false);
        //
        byte[] serialized = serializer.serialize(response);
        NullResponse deserialized = serializer.deserialize(serialized);
        assertNotNull(deserialized);
        assertEquals(response, deserialized);
    }

    @Test
    public void testNullResponseWithErrorMessage() throws IOException, ClassNotFoundException {
        NullResponse response = new NullResponse("id", new ErrorMessage(-1, "error"));
        //
        MsgPackSerializer<NullResponse> serializer = new MsgPackSerializer<NullResponse>(false);
        //
        byte[] serialized = serializer.serialize(response);
        NullResponse deserialized = serializer.deserialize(serialized);
        assertNotNull(deserialized);
        assertEquals(response, deserialized);
    }
}
