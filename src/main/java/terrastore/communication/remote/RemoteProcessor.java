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
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.StaticChannelPipeline;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.communication.process.AbstractProcessor;
import terrastore.communication.ProcessingException;
import terrastore.communication.process.AsynchronousExecutor;
import terrastore.communication.protocol.Command;
import terrastore.communication.process.CompletionHandler;
import terrastore.communication.process.RouterHandler;
import terrastore.communication.protocol.NullResponse;
import terrastore.communication.protocol.Response;
import terrastore.router.Router;
import terrastore.util.io.MsgPackSerializer;

/**
 * Process {@link terrastore.communication.protocol.Command} messages sent by remote cluster nodes.
 *
 * @author Sergio Bossa
 */
public class RemoteProcessor extends AbstractProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteProcessor.class);
    //
    private final Lock stateLock = new ReentrantLock();
    private final String host;
    private final int port;
    private final ServerBootstrap server;
    private final ChannelGroup acceptedChannels;
    private final Router router;
    private Channel serverChannel;

    public RemoteProcessor(String host, int port, int threads, boolean compressCommunication, Router router) {
        super(new AsynchronousExecutor(threads));
        this.host = host;
        this.port = port;
        this.router = router;
        acceptedChannels = new DefaultChannelGroup(this.toString());
        server = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        server.setOption("reuseAddress", true);
        server.setPipelineFactory(new ServerChannelPipelineFactory(new ServerHandler(), compressCommunication));
    }

    protected void doStart() {
        stateLock.lock();
        try {
            if (serverChannel == null) {
                serverChannel = server.bind(new InetSocketAddress(host, port));
                acceptedChannels.add(serverChannel);
                LOG.debug("Bound channel to: {}:{}", host, port);
            } else {
                throw new IllegalStateException("Request to bind an already active channel!");
            }
        } finally {
            stateLock.unlock();
        }
    }

    protected void doStop() {
        stateLock.lock();
        try {
            if (serverChannel != null) {
                acceptedChannels.close().awaitUninterruptibly();
                server.releaseExternalResources();
                serverChannel = null;
                LOG.debug("Unbound channel from: {}:{}", host, port);
            } else {
                throw new IllegalStateException("Request to unbind an inactive channel!");
            }
        } finally {
            stateLock.unlock();
        }
    }

    @Sharable
    private class ServerHandler extends SimpleChannelUpstreamHandler {

        @Override
        public void channelOpen(ChannelHandlerContext context, ChannelStateEvent event) throws Exception {
            acceptedChannels.add(event.getChannel());
        }

        @Override
        public void messageReceived(ChannelHandlerContext context, MessageEvent event) throws Exception {
            try {
                Channel channel = event.getChannel();
                Command command = (Command) event.getMessage();
                String commandId = command.getId();
                process(command, new RouterHandler(router), new RemoteCompletionHandler(channel, commandId));
            } catch (ClassCastException ex) {
                LOG.warn("Unexpected command of type: " + event.getMessage().getClass());
                throw new IllegalStateException("Unexpected command of type: " + event.getMessage().getClass());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) throws Exception {
            LOG.error(event.getCause().getMessage(), event.getCause());
        }
    }

    private static class ServerChannelPipelineFactory implements ChannelPipelineFactory {

        private final ServerHandler serverHandler;
        private final boolean compressCommunication;

        public ServerChannelPipelineFactory(ServerHandler serverHandler, boolean compressCommunication) {
            this.serverHandler = serverHandler;
            this.compressCommunication = compressCommunication;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = new StaticChannelPipeline(
                    new LengthFieldPrepender(4),
                    new SerializerEncoder(new MsgPackSerializer(compressCommunication)),
                    new SerializerDecoder(new MsgPackSerializer(compressCommunication)),
                    serverHandler);
            return pipeline;
        }
    }

    private static class RemoteCompletionHandler implements CompletionHandler<Object, ProcessingException> {

        private final Channel channel;
        private final String commandId;

        public RemoteCompletionHandler(Channel channel, String commandId) {
            this.channel = channel;
            this.commandId = commandId;
        }

        @Override
        public void handleSuccess(Response response) throws Exception {
            channel.write(response);
        }

        @Override
        public void handleFailure(ProcessingException exception) throws Exception {
            channel.write(new NullResponse(commandId, exception.getErrorMessage()));
        }
    }
}
