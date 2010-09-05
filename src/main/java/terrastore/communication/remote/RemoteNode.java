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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.RemoteNodeFactory;
import terrastore.communication.protocol.Command;
import terrastore.communication.remote.serialization.JavaSerializer;

/**
 * Send {@link terrastore.communication.protocol.Command} messages to remote cluster nodes, waiting for the asynchronous response.<br>
 * Upon disconnection of the actual remote host, pending commands will fail and an error response will be returned.
 *
 * @author Sergio Bossa
 */
public class RemoteNode implements Node {

    private static final transient Logger LOG = LoggerFactory.getLogger(RemoteNode.class);
    //
    private final Lock stateLock = new ReentrantLock();
    private final ConcurrentMap<String, CountDownLatch> responseConditions = new ConcurrentHashMap<String, CountDownLatch>();
    private final ConcurrentMap<String, RemoteResponse> responses = new ConcurrentHashMap<String, RemoteResponse>();
    private final String host;
    private final int port;
    private final String name;
    private final int maxFrameLength;
    private final long timeoutInMillis;
    private volatile ClientBootstrap client;
    private volatile Channel clientChannel;
    private volatile boolean connected;

    protected RemoteNode(String host, int port, String name, int maxFrameLength, long timeoutInMillis) {
        this.host = host;
        this.port = port;
        this.name = name;
        this.maxFrameLength = maxFrameLength;
        this.timeoutInMillis = timeoutInMillis;
    }

    @Override
    public void connect() {
        stateLock.lock();
        try {
            if (!connected) {
                client = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
                client.setPipelineFactory(new ClientChannelPipelineFactory(maxFrameLength, new ClientHandler()));
                ChannelFuture future = client.connect(new InetSocketAddress(host, port));
                future.awaitUninterruptibly(timeoutInMillis, TimeUnit.MILLISECONDS);
                if (future.isSuccess()) {
                    LOG.info("Connected to {}:{}", host, port);
                    clientChannel = future.getChannel();
                    connected = true;
                } else {
                    throw new RuntimeException("Error connecting to " + host + ":" + port, future.getCause());
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void disconnect() {
        stateLock.lock();
        try {
            if (connected) {
                clientChannel.close().awaitUninterruptibly();
                client.releaseExternalResources();
                connected = false;
                LOG.info("Disconnected from : {}:{}", host, port);
            }
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public <R> R send(Command<R> command) throws CommunicationException, ProcessingException {
        if (!connected) {
            connect();
        }
        if (clientChannel.isOpen() && clientChannel.isConnected()) {
            String commandId = configureId(command);
            try {
                CountDownLatch responseLatch = new CountDownLatch(1);
                responseConditions.put(commandId, responseLatch);
                clientChannel.write(command);
                LOG.debug("Sent command {}", commandId);
                //
                long wait = timeoutInMillis;
                while (!responses.containsKey(commandId) && wait > 0) {
                    long start = System.currentTimeMillis();
                    try {
                        responseLatch.await(wait, TimeUnit.MILLISECONDS);
                        wait = 0;
                    } catch (InterruptedException ex) {
                        wait = wait - (System.currentTimeMillis() - start);
                    }
                }
                //
                RemoteResponse response = responses.get(commandId);
                if (response != null && response.isOk()) {
                    // Safe cast: correlation id ensures it's the *correct* command response.
                    return (R) response.getResult();
                    //
                } else if (response != null) {
                    throw new ProcessingException(response.getError());
                } else {
                    throw new CommunicationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Communication timeout!"));
                }
            } finally {
                responses.remove(commandId);
            }
        } else {
            throw new CommunicationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Communication error!"));
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    public boolean equals(Object obj) {
        if (obj != null && obj instanceof RemoteNode) {
            RemoteNode other = (RemoteNode) obj;
            return this.name.equals(other.name);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
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
    private class ClientHandler extends SimpleChannelUpstreamHandler {

        @Override
        public void messageReceived(ChannelHandlerContext context, MessageEvent event) throws Exception {
            try {
                RemoteResponse response = (RemoteResponse) event.getMessage();
                String correlationId = response.getCorrelationId();
                signalCommandResponse(correlationId, response);
            } catch (ClassCastException ex) {
                LOG.warn("Unexpected response of type: " + event.getMessage().getClass());
                throw new IllegalStateException("Unexpected response of type: " + event.getMessage().getClass());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) throws Exception {
            LOG.debug(event.getCause().getMessage(), event.getCause());
        }

        private void signalCommandResponse(String commandId, RemoteResponse response) {
            CountDownLatch responseLatch = responseConditions.remove(commandId);
            responses.put(commandId, response);
            responseLatch.countDown();
        }
    }

    private static class ClientChannelPipelineFactory implements ChannelPipelineFactory {

        private final int maxFrameLength;
        private final ClientHandler clientHandler;

        public ClientChannelPipelineFactory(int maxFrameLength, ClientHandler clientHandler) {
            this.maxFrameLength = maxFrameLength;
            this.clientHandler = clientHandler;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("LENGTH_HEADER_PREPENDER", new LengthFieldPrepender(4));
            pipeline.addLast("LENGTH_HEADER_DECODER", new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4));
            pipeline.addLast("COMMAND_ENCODER", new SerializerEncoder(new JavaSerializer<Command>()));
            pipeline.addLast("RESPONSE_DECODER", new SerializerDecoder(new JavaSerializer<RemoteResponse>()));
            pipeline.addLast("HANDLER", clientHandler);
            return pipeline;
        }
    }

    public static class Factory implements RemoteNodeFactory {

        private int defaultMaxFrameLength;
        private int defaultNodeTimeout;

        @Override
        public Node makeRemoteNode(String host, int port, String name) {
            return new RemoteNode(host, port, name, defaultMaxFrameLength, defaultNodeTimeout);
        }

        @Override
        public RemoteNode makeRemoteNode(String host, int port, String name, int maxFrameLength, long nodeTimeout) {
            return new RemoteNode(host, port, name, maxFrameLength, nodeTimeout);
        }

        public void setDefaultMaxFrameLength(int defaultMaxFrameLength) {
            this.defaultMaxFrameLength = defaultMaxFrameLength;
        }

        public void setDefaultNodeTimeout(int defaultNodeTimeout) {
            this.defaultNodeTimeout = defaultNodeTimeout;
        }
    }
}
