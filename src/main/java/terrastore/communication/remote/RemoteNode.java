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
package terrastore.communication.remote;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.StaticChannelPipeline;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.communication.NodeConfiguration;
import terrastore.common.ErrorMessage;
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.RemoteNodeFactory;
import terrastore.communication.protocol.Command;
import terrastore.communication.protocol.Response;
import terrastore.util.io.MsgPackSerializer;

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
    private final ConcurrentMap<String, SynchronousQueue<Response>> rendezvous = new ConcurrentHashMap<String, SynchronousQueue<Response>>();
    private final NodeConfiguration configuration;
    private final boolean compressCommunication;
    private final long timeoutInMillis;
    private volatile ClientBootstrap client;
    private volatile Channel clientChannel;
    private volatile boolean connected;

    protected RemoteNode(NodeConfiguration configuration, long timeoutInMillis, boolean compressCommunication) {
        this.configuration = configuration;
        this.timeoutInMillis = timeoutInMillis;
        this.compressCommunication = compressCommunication;
    }

    @Override
    public void connect() {
        stateLock.lock();
        try {
            if (!connected) {
                client = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
                client.setPipelineFactory(new ClientChannelPipelineFactory(new ClientHandler(), compressCommunication));
                ChannelFuture future = tryConnect();
                if (future.isSuccess()) {
                    clientChannel = future.getChannel();
                    connected = true;
                    LOG.debug("Connected to remote node {}", clientChannel.getRemoteAddress());
                } else {
                    StringBuilder addresses = new StringBuilder();
                    for (String address : configuration.getNodePublishHosts()) {
                        if (addresses.length() > 0) {
                            addresses.append(",");
                        }
                        addresses.append(address);
                    }
                    throw new RuntimeException("Error connecting to the following addresses: " + addresses.toString());
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
                LOG.debug("Disconnected from remote node {}", clientChannel.getRemoteAddress());
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
        String commandId = configureId(command);
        try {
            SynchronousQueue<Response> channel = new SynchronousQueue<Response>();
            rendezvous.put(commandId, channel);
            clientChannel.write(command);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sent command {} to remote node {}", commandId, clientChannel.getRemoteAddress());
            }
            //
            Response response = null;
            long wait = timeoutInMillis;
            do {
                long start = System.currentTimeMillis();
                try {
                    response = channel.poll(wait, TimeUnit.MILLISECONDS);
                    wait = 0;
                } catch (InterruptedException ex) {
                    wait = wait - (System.currentTimeMillis() - start);
                }
            } while (response == null && wait > 0);
            //
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
            rendezvous.remove(commandId);
        }
    }

    @Override
    public String getName() {
        return configuration.getName();
    }

    @Override
    public String getHost() {
        return ((InetSocketAddress) clientChannel.getRemoteAddress()).getHostName();
    }

    @Override
    public int getPort() {
        return ((InetSocketAddress) clientChannel.getRemoteAddress()).getPort();
    }

    @Override
    public NodeConfiguration getConfiguration() {
        return configuration;
    }

    public boolean equals(Object obj) {
        if (obj != null && obj instanceof RemoteNode) {
            RemoteNode other = (RemoteNode) obj;
            return this.configuration.getName().equals(other.configuration.getName());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return configuration.getName().hashCode();
    }

    @Override
    public String toString() {
        return configuration.getName();
    }

    private ChannelFuture tryConnect() throws RuntimeException {
        ChannelFuture future = null;
        for (String address : configuration.getNodePublishHosts()) {
            future = client.connect(new InetSocketAddress(address, configuration.getNodePort()));
            future.awaitUninterruptibly(timeoutInMillis, TimeUnit.MILLISECONDS);
            if (future.isSuccess()) {
                break;
            }
        }
        return future;
    }

    private String configureId(Command command) {
        // FIXME: extract id generation strategy (possibly making it more lightweight)
        String commandId = UUID.randomUUID().toString();
        //
        command.setId(commandId);
        //
        return commandId;
    }

    @Sharable
    private class ClientHandler extends SimpleChannelUpstreamHandler {

        @Override
        public void messageReceived(ChannelHandlerContext context, MessageEvent event) throws Exception {
            try {
                Response response = (Response) event.getMessage();
                String correlationId = response.getCorrelationId();
                signalCommandResponse(correlationId, response);
            } catch (ClassCastException ex) {
                LOG.warn("Unexpected response of type: " + event.getMessage().getClass());
                throw new IllegalStateException("Unexpected response of type: " + event.getMessage().getClass());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) throws Exception {
            LOG.error(event.getCause().getMessage(), event.getCause());
        }

        private void signalCommandResponse(String commandId, Response response) {
            try {
                SynchronousQueue<Response> channel = rendezvous.remove(commandId);
                // Heuristically waits for 1 sec in case response arrived prior to the sending thread started listening for it:
                boolean offered = channel.offer(response, 1000, TimeUnit.MILLISECONDS);
                if (!offered) {
                    LOG.warn("No consumer thread found, response for command {} is going to be ignored.", commandId);
                }
            } catch (InterruptedException ex) {
                LOG.warn("No consumer thread found, response for command {} is going to be ignored.", commandId);
            }
        }

    }

    private static class ClientChannelPipelineFactory implements ChannelPipelineFactory {

        private final ClientHandler clientHandler;
        private final boolean compressCommunication;

        public ClientChannelPipelineFactory(ClientHandler clientHandler, boolean compressCommunication) {
            this.clientHandler = clientHandler;
            this.compressCommunication = compressCommunication;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = new StaticChannelPipeline(
                    new LengthFieldPrepender(4),
                    new SerializerEncoder(new MsgPackSerializer(compressCommunication)),
                    new SerializerDecoder(new MsgPackSerializer(compressCommunication)),
                    clientHandler);
            return pipeline;
        }

    }

    public static class Factory implements RemoteNodeFactory {

        private static final long DEFAULT_NODE_TIMEOUT = 10000;
        private static final boolean DEFAULT_COMPRESS_COMMUNICATION = false;

        @Override
        public Node makeRemoteNode(NodeConfiguration configuration) {
            return new RemoteNode(configuration, DEFAULT_NODE_TIMEOUT, DEFAULT_COMPRESS_COMMUNICATION);
        }

        @Override
        public RemoteNode makeRemoteNode(NodeConfiguration configuration, long nodeTimeout, boolean compressCommunication) {
            return new RemoteNode(configuration, nodeTimeout, compressCommunication);
        }

    }
}
