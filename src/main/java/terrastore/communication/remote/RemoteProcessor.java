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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.communication.protocol.Command;
import terrastore.communication.protocol.Response;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;

/**
 * Process {@link terrastore.communication.protocol.Command} messages sent by remote cluster nodes.
 *
 * @author Sergio Bossa
 */
public class RemoteProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteProcessor.class);
    private final Lock stateLock = new ReentrantLock();
    private final String host;
    private final int port;
    private final Store store;
    private final ExecutorService commandExecutor;
    private final ServerBootstrap server;
    private final ChannelGroup acceptedChannels;
    private Channel serverChannel;

    public RemoteProcessor(String host, int port, Store store, ExecutorService commandExecutor) {
        this.host = host;
        this.port = port;
        this.store = store;
        this.commandExecutor = commandExecutor;
        acceptedChannels = new DefaultChannelGroup(this.toString());
        server = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newSingleThreadExecutor(), commandExecutor));
        server.setOption("reuseAddress", true);
        server.getPipeline().addLast(ObjectEncoder.class.getName(), new ObjectEncoder());
        server.getPipeline().addLast(ObjectDecoder.class.getName(), new ObjectDecoder());
        server.getPipeline().addLast(ServerHandler.class.getName(), new ServerHandler());
    }

    public void start() {
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

    public void stop() {
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
    private class ServerHandler extends SimpleChannelHandler {

        @Override
        public void channelOpen(ChannelHandlerContext context, ChannelStateEvent event) throws Exception {
            acceptedChannels.add(event.getChannel());
        }

        @Override
        public void messageReceived(ChannelHandlerContext context, MessageEvent event) throws Exception {
            try {
                Command command = (Command) event.getMessage();
                try {
                    Map<String, Value> entries = command.executeOn(store);
                    event.getChannel().write(new Response(command.getId(), entries));
                } catch (StoreOperationException ex) {
                    event.getChannel().write(new Response(command.getId(), ex.getErrorMessage()));
                }
            } catch (ClassCastException ex) {
                LOG.warn("Unexpected command of type: " + event.getMessage().getClass());
                throw new IllegalStateException("Unexpected message of type: " + event.getMessage().getClass());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event) throws Exception {
            LOG.debug(event.getCause().getMessage(), event.getCause());
        }
    }
}
