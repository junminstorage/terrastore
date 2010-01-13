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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.terracotta.modules.annotations.InstrumentedClass;
import terrastore.communication.protocol.Command;
import terrastore.communication.protocol.Response;
import terrastore.communication.serialization.Serializer;

/**
 * Topology of {@link Pipe} instances, holding two different sets of pipes: <i>command pipes</i>, used for sending
 * {@link terrastore.communication.protocol.Command} messages between remote cluster nodes, and <i>response pipes</i>,
 * used for sending back {@link terrastore.communication.protocol.Response} messages.
 *
 * @author Sergio Bossa
 */
@InstrumentedClass
public class Topology {

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final Map<String, Pipe<Command>> commandPipes = new HashMap<String, Pipe<Command>>();
    private final Map<String, Pipe<Response>> responsePipes = new HashMap<String, Pipe<Response>>();
    private final Serializer<Command> commandSerializer;
    private final Serializer<Response> responseSerializer;

    public Topology(Serializer<Command> commandSerializer, Serializer<Response> responseSerializer) {
        this.commandSerializer = commandSerializer;
        this.responseSerializer = responseSerializer;
    }

    public Pipe<Command> getOrCreateCommandPipe(String pipeName) {
        return this.<Command>getOrCreatePipe(commandPipes, pipeName, commandSerializer);
    }

    public Pipe<Response> getOrCreateResponsePipe(String pipeName) {
        return this.<Response>getOrCreatePipe(responsePipes, pipeName, responseSerializer);
    }

    public Pipe<Command> getCommandPipe(String pipeName) {
        return this.<Command>getPipe(commandPipes, pipeName);
    }

    public Pipe<Response> getResponsePipe(String pipeName) {
        return this.<Response>getPipe(responsePipes, pipeName);
    }

    private <T> Pipe<T> getOrCreatePipe(Map<String, Pipe<T>> pipes, String pipeName, Serializer<T> pipeSerializer) {
        stateLock.writeLock().lock();
        try {
            Pipe<T> pipe = pipes.get(pipeName);
            if (pipe == null) {
                pipe = new Pipe<T>(pipeSerializer);
                pipes.put(pipeName, pipe);
            }
            return pipe;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    private <T> Pipe<T> getPipe(Map<String, Pipe<T>> pipes, String pipeName) {
        stateLock.readLock().lock();
        try {
            return pipes.get(pipeName);
        } finally {
            stateLock.readLock().unlock();
        }
    }
}
