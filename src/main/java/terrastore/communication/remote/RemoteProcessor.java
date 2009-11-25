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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.communication.remote.pipe.PipeListener;
import terrastore.communication.remote.pipe.Topology;
import terrastore.communication.protocol.Command;
import terrastore.communication.protocol.Response;
import terrastore.communication.remote.pipe.Pipe;
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
    private final String name;
    private final Topology pipes;
    private final Store store;
    private final ExecutorService commandExecutor;
    private volatile PipeListener<Command> commandListener;

    public RemoteProcessor(String name, Topology pipes, Store store, ExecutorService commandExecutor) {
        this.name = name;
        this.pipes = pipes;
        this.store = store;
        this.commandExecutor = commandExecutor;
    }

    public void start() {
        commandListener = new CommandListener();
        commandListener.start();
    }

    public void stop() {
        commandListener.abort();
    }

    private class CommandListener extends PipeListener<Command> {

        public CommandListener() {
            super(pipes.getOrCreateCommandPipe(name));
        }

        @Override
        public void onMessage(final Command command) throws Exception {
            commandExecutor.submit(new Runnable() {

                public void run() {
                    Pipe<Response> responsePipe = pipes.getResponsePipe(command.getSender());
                    try {
                        Map<String, Value> entries = command.executeOn(store);
                        responsePipe.put(new Response(command.getId(), entries));
                    } catch (InterruptedException ex) {
                        LOG.error(ex.getMessage(), ex);
                    } catch (StoreOperationException ex) {
                        try {
                            responsePipe.put(new Response(command.getId(), ex.getErrorMessage()));
                        } catch (InterruptedException innerEx) {
                            LOG.error(innerEx.getMessage(), innerEx);
                        }
                    }
                }
            });
        }
    }
}
