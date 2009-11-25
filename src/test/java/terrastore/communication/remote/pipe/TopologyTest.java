/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.communication.remote.pipe;

import org.junit.Test;
import terrastore.communication.protocol.Command;
import terrastore.communication.protocol.Response;
import terrastore.communication.serialization.Serializer;
import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class TopologyTest {

    private final String COMMAND_PIPE = "commandPipe";
    private final String RESPONSE_PIPE = "responsePipe";

    @Test
    public void testGetOrCreateCommandPipe() {
        Serializer<Command> commandSerializer = createMock(Serializer.class);
        Serializer<Response> responseSerializer = createMock(Serializer.class);

        replay(commandSerializer, responseSerializer);

        Topology topology = new Topology(commandSerializer, responseSerializer);
        Pipe commandPipe = topology.getCommandPipe(COMMAND_PIPE);
        assertNull(commandPipe);
        commandPipe = topology.getOrCreateCommandPipe(COMMAND_PIPE);
        assertNotNull(commandPipe);
        assertSame(commandPipe, topology.getOrCreateCommandPipe(COMMAND_PIPE));

        verify(commandSerializer, responseSerializer);
    }

    @Test
    public void testGetOrCreateResponsePipe() {
        Serializer<Command> commandSerializer = createMock(Serializer.class);
        Serializer<Response> responseSerializer = createMock(Serializer.class);

        replay(commandSerializer, responseSerializer);

        Topology topology = new Topology(commandSerializer, responseSerializer);
        Pipe responsePipe = topology.getResponsePipe(RESPONSE_PIPE);
        assertNull(responsePipe);
        responsePipe = topology.getOrCreateResponsePipe(RESPONSE_PIPE);
        assertNotNull(responsePipe);
        assertSame(responsePipe, topology.getOrCreateResponsePipe(RESPONSE_PIPE));

        verify(commandSerializer, responseSerializer);
    }
}