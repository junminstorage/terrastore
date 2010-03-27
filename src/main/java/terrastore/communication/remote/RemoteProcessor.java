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
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.communication.seda.AbstractSEDAProcessor;
import terrastore.communication.ProcessingException;
import terrastore.communication.Processor;
import terrastore.communication.protocol.Command;
import terrastore.communication.remote.serialization.JavaSerializer;
import terrastore.communication.seda.RoutingHandler;
import terrastore.router.Router;

/**
 * Process {@link terrastore.communication.protocol.Command} messages sent by remote cluster nodes.
 *
 * @author Sergio Bossa
 */
public class RemoteProcessor extends AbstractSEDAProcessor implements Processor {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteProcessor.class);
    //
    private final Lock stateLock = new ReentrantLock();
    private final String host;
    private final int port;
    private final ServerBootstrap server;
    private final ChannelGroup acceptedChannels;
    private final Router router;
    private Channel serverChannel;

    public RemoteProcessor(String host, int port, int maxFrameLength, int threads, Router router) {
        super(threads);
        this.host = host;
        this.port = port;
        this.router = router;
        acceptedChannels = new DefaultChannelGroup(this.toString());
        server = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        server.setOption("reuseAddress", true);
        server.setPipelineFactory(new ServerChannelPipelineFactory(maxFrameLength, new ServerHandler()));
    }

    @Override
    public <R> R process(Command<R> command) throws ProcessingException {
        return process(command, new RoutingHandler<R>(router));
    }

    protected void doStart() {
        stateLock.lock();
        try {
            if (serverChannel == null) {
                serverChannel = server.bind(new InetSocketAddress(host, port));
                acceptedChannels.add(serverChannel);
                LOG.info("Bound channel to: {}:{}", host, port);
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
                LOG.info("Unbound channel from: {}:{}", host, port);
            } else {
                throw new IllegalStateException("Request to unbind an inactive channel!");
            }
        } finally {
            stateLock.unlock();
        }
    }

    @ChannelPipelineCoverage("all")
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
                try {
                    Object result = process(command);
                    channel.write(new RemoteResponse(command.getId(), result));
                } catch (ProcessingException ex) {
                    channel.write(new RemoteResponse(command.getId(), ex.getErrorMessage()));
                }
            } catch (ClassCastException ex) {
                LOG.warn("Unexpected command of type: " + event.getMessage().getClass());
                throw new IllegalStateException("Unexpected command of type: " + event.getMessage().getClass());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) throws Exception {
            LOG.debug(event.getCause().getMessage(), event.getCause());
        }
    }

    private static class ServerChannelPipelineFactory implements ChannelPipelineFactory {

        private final int maxFrameLength;
        private final ServerHandler serverHandler;

        public ServerChannelPipelineFactory(int maxFrameLength, ServerHandler serverHandler) {
            this.maxFrameLength = maxFrameLength;
            this.serverHandler = serverHandler;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("LENGTH_HEADER_PREPENDER", new LengthFieldPrepender(4));
            pipeline.addLast("LENGTH_HEADER_DECODER", new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4));
            pipeline.addLast("RESPONSE_ENCODER", new SerializerEncoder(new JavaSerializer<RemoteResponse>()));
            pipeline.addLast("COMMAND_DECODER", new SerializerDecoder(new JavaSerializer<Command>()));
            pipeline.addLast("HANDLER", serverHandler);
            return pipeline;
        }
    }
}
