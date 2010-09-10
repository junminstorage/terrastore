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
package terrastore.communication.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.cluster.coordinator.ServerConfiguration;
import terrastore.communication.CommunicationException;
import terrastore.communication.LocalNodeFactory;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.Command;

/**
 * Local {@link terrastore.communication.Node} implementation representing <b>this</b> cluster node instance.<br>
 * <br>
 * All  {@link terrastore.communication.protocol.Command} messages sent to a local node are synchronously executed
 * in the same virtual machine.
 *
 * @author Sergio Bossa
 */
public class LocalNode implements Node {

    private static final Logger LOG = LoggerFactory.getLogger(LocalNode.class);
    private final ServerConfiguration configuration;
    private final LocalProcessor processor;

    protected LocalNode(ServerConfiguration configuration, LocalProcessor processor) {
        this.configuration = configuration;
        this.processor = processor;
    }

    @Override
    public void connect() {
    }

    @Override
    public <R> R send(Command<R> command) throws CommunicationException, ProcessingException {
        R result = processor.<R>process(command);
        return result;
    }

    @Override
    public void disconnect() {
    }

    @Override
    public String getName() {
        return configuration.getName();
    }

    @Override
    public String getHost() {
        return configuration.getNodeHost();
    }

    @Override
    public int getPort() {
        return configuration.getNodePort();
    }

    @Override
    public ServerConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof LocalNode) {
            LocalNode other = (LocalNode) obj;
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

    public static class Factory implements LocalNodeFactory {

        @Override
        public Node makeLocalNode(ServerConfiguration configuration, LocalProcessor processor) {
            return new LocalNode(configuration, processor);
        }
    }
}
