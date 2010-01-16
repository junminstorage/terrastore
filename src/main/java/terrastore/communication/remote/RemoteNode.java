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

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.Command;
import terrastore.communication.remote.serialization.JavaSerializer;

/**
 * Send {@link terrastore.communication.protocol.Command} messages to remote cluster nodes.
 *
 * @author Sergio Bossa
 */
public class RemoteNode implements Node {

    private static final transient Logger LOG = LoggerFactory.getLogger(RemoteNode.class);
    private final Lock stateLock = new ReentrantLock();
    private final Map<String, Condition> responseConditions = new HashMap<String, Condition>();
    private final Map<String, RemoteResponse> responses = new HashMap<String, RemoteResponse>();
    private final String host;
    private final int port;
    private final String name;
    private final long timeoutInMillis;
    private final ClientBootstrap client;
    private Channel clientChannel;

    public RemoteNode(String host, int port, String name, long timeoutInMillis) {
        this.host = host;
        this.port = port;
        this.name = name;
        this.timeoutInMillis = timeoutInMillis;
        client = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor()));
        client.getPipeline().addLast("LENGTH_HEADER_PREPENDER", new LengthFieldPrepender(4));
        client.getPipeline().addLast("LENGTH_HEADER_DECODER", new LengthFieldBasedFrameDecoder(3145728, 0, 4, 0, 4));
        client.getPipeline().addLast("COMMAND_ENCODER", new SerializerEncoder(new JavaSerializer<Command>()));
        client.getPipeline().addLast("RESPONSE_DECODER", new SerializerDecoder(new JavaSerializer<RemoteResponse>()));
        client.getPipeline().addLast("HANDLER", new ClientHandler());
    }

    public void connect() {
        stateLock.lock();
        try {
            if (clientChannel == null) {
                ChannelFuture future = client.connect(new InetSocketAddress(host, port));
                future.awaitUninterruptibly(timeoutInMillis, TimeUnit.MILLISECONDS);
                if (future.isSuccess()) {
                    LOG.info("Connected to {}:{}", host, port);
                    clientChannel = future.getChannel();
                } else {
                    throw new RuntimeException("Error connecting to " + host + ":" + port, future.getCause());
                }
            } else {
                throw new IllegalStateException("Already connected to " + host + ":" + port);
            }
        } finally {
            stateLock.unlock();
        }
    }

    public void disconnect() {
        stateLock.lock();
        try {
            if (clientChannel != null) {
                clientChannel.close();
                client.releaseExternalResources();
                clientChannel = null;
                LOG.info("Disconnected from : {}:{}", host, port);
            }
        } finally {
            stateLock.unlock();
        }
    }

    public <R> R send(Command<R> command) throws ProcessingException {
        stateLock.lock();
        if (clientChannel != null) {
            try {
                String commandId = configureId(command);
                Condition responseReceived = stateLock.newCondition();
                responseConditions.put(commandId, responseReceived);
                clientChannel.write(command);
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
                RemoteResponse response = responses.remove(commandId);
                if (response != null && response.isOk()) {
                    // Safe cast: correlation id ensures it's the *correct* command response.
                    return (R) response.getResult();
                    //
                } else if (response != null) {
                    throw new ProcessingException(response.getError());
                } else {
                    throw new ProcessingException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Communication timeout!"));
                }
            } finally {
                stateLock.unlock();
            }
        } else {
            throw new IllegalStateException();
        }
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

    private String configureId(Command command) {
        // FIXME: extract id generation strategy (possibly making it more lightweight)
        String commandId = UUID.randomUUID().toString();
        //
        command.setId(commandId);
        //
        return commandId;
    }

    private long millisToNanos(long time) {
        return TimeUnit.MILLISECONDS.toNanos(time);
    }

    @ChannelPipelineCoverage("all")
    private class ClientHandler extends SimpleChannelHandler {

        @Override
        public void messageReceived(ChannelHandlerContext context, MessageEvent event) throws Exception {
            stateLock.lock();
            try {
                RemoteResponse response = (RemoteResponse) event.getMessage();
                String correlationId = response.getCorrelationId();
                Condition responseCondition = responseConditions.remove(correlationId);
                responses.put(correlationId, response);
                responseCondition.signal();
                LOG.debug("Received response for command {}", correlationId);
            } catch (ClassCastException ex) {
                LOG.warn("Unexpected command of type: " + event.getMessage().getClass());
                throw new IllegalStateException("Unexpected command of type: " + event.getMessage().getClass());
            } finally {
                stateLock.unlock();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) throws Exception {
            LOG.debug(event.getCause().getMessage(), event.getCause());
        }
    }
}
