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
package terrastore.communication.remote;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.remote.pipe.Topology;
import terrastore.communication.protocol.Command;
import terrastore.communication.protocol.Response;
import terrastore.communication.remote.pipe.Pipe;
import terrastore.communication.remote.pipe.PipeListener;
import terrastore.store.Value;

/**
 * Remote {@link terrastore.communication.Node} implementation acting as a proxy toward a remote cluster node.<br>
 * <br>
 * All {@link terrastore.communication.protocol.Command} messages sent to a remote node are enqueued into a communication
 * {@link terrastore.communication.remote.pipe.Pipe} and remotely received and executed by a {@link RemoteProcessor}, which will then send the
 * {@link terrastore.communication.protocol.Response} back to the original node through another communication
 * {@link terrastore.communication.remote.pipe.Pipe}.<br>
 * <br>
 * Each RemoteNode proxy object communicates with its actual remote node through its own pair of pipes.
 *
 * @author Sergio Bossa
 */
public class RemoteNode implements Node {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteNode.class);
    private final String id;
    private final String name;
    private final Map<String, Condition> responseConditions = new HashMap<String, Condition>();
    private final Map<String, Response> responses = new HashMap<String, Response>();
    private final ReentrantLock stateLock = new ReentrantLock();
    private final Topology pipes;
    private final long timeoutInMillis;
    private volatile Pipe<Command> commandPipe;
    private volatile ResponseListener responseListener;

    public RemoteNode(String name, Topology pipes, long timeoutInMillis) {
        this.id = UUID.randomUUID().toString();
        this.name = name;
        this.pipes = pipes;
        this.timeoutInMillis = timeoutInMillis;
    }

    public void connect() {
        commandPipe = pipes.getOrCreateCommandPipe(name);
        responseListener = new ResponseListener();
        responseListener.start();
    }

    public Map<String, Value> send(Command command) throws ProcessingException {
        stateLock.lock();
        try {
            configure(command);
            String commandId = command.getId();
            Condition responseReceived = stateLock.newCondition();
            responseConditions.put(commandId, responseReceived);
            commandPipe.put(command);
            LOG.debug("Sent command {}", commandId);
            //
            long wait = millisToNanos(timeoutInMillis);
            while (!responses.containsKey(commandId) && wait > 0) {
                long start = millisToNanos(System.currentTimeMillis());
                try {
                    wait = responseReceived.awaitNanos(wait);
                } catch (InterruptedException ex) {
                    wait = wait - (millisToNanos(System.currentTimeMillis()) - start);
                }
            }
            //
            Response response = responses.remove(commandId);
            if (response != null && response.isOk()) {
                Map<String, Value> entries = response.getEntries();
                return entries;
            } else if (response != null) {
                throw new ProcessingException(response.getError());
            } else {
                throw new ProcessingException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Communication timeout!"));
            }
        } catch (InterruptedException ex) {
            throw new IllegalStateException(ex);
        } finally {
            stateLock.unlock();
        }
    }

    public void disconnect() {
        stateLock.lock();
        try {
            responseConditions.clear();
            responses.clear();
        } finally {
            stateLock.unlock();
        }
        responseListener.abort();
    }

    public String getName() {
        return name;
    }

    public boolean equals(Object obj) {
        if (obj != null && obj instanceof RemoteNode) {
            RemoteNode other = (RemoteNode) obj;
            return this.name.equals(other.name);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return name.hashCode();
    }

    public String toString() {
        return name;
    }

    private void configure(Command command) {
        // FIXME: extract id generation strategy (possibly making it more lightweight)
        String commandId = UUID.randomUUID().toString();
        // FIXME: do not compute sender again and again
        String commandSender = getSender();
        //
        command.setId(commandId);
        command.setSender(commandSender);
    }

    private String getSender() {
        return name + "-" + id;
    }

    private long millisToNanos(long time) {
        return TimeUnit.MILLISECONDS.toNanos(time);
    }

    private class ResponseListener extends PipeListener<Response> {

        public ResponseListener() {
            super(pipes.getOrCreateResponsePipe(getSender()));
        }

        @Override
        public void onMessage(Response response) throws Exception {
            stateLock.lock();
            try {
                String correlationId = response.getCorrelationId();
                Condition responseCondition = responseConditions.remove(correlationId);
                responses.put(correlationId, response);
                responseCondition.signal();
                LOG.debug("Received response for command {}", correlationId);
            } finally {
                stateLock.unlock();
            }
        }
    }
}
