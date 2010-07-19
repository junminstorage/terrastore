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
    private final String host;
    private final int port;
    private final String name;
    private final LocalProcessor processor;

    protected LocalNode(String host, int port, String name, LocalProcessor processor) {
        this.host = host;
        this.port = port;
        this.name = name;
        this.processor = processor;
    }

    @Override
    public void connect() {
    }

    @Override
    public <R> R send(Command<R> command) throws ProcessingException {
        R result = processor.<R>process(command);
        return result;
    }

    @Override
    public void disconnect() {
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

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof LocalNode) {
            LocalNode other = (LocalNode) obj;
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

    public static class Factory implements LocalNodeFactory {

        @Override
        public Node makeLocalNode(String host, int port, String name, LocalProcessor processor) {
            return new LocalNode(host, port, name, processor);
        }
    }
}
