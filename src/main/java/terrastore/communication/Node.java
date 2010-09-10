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
package terrastore.communication;

import terrastore.cluster.coordinator.ServerConfiguration;
import terrastore.communication.protocol.Command;

/**
 * Node interface, representing an actual node in a cluster.
 * <br><br>
 * A node can be local ({@link terrastore.communication.local.LocalNode}) or remote ({@link terrastore.communication.remote.RemoteNode}).
 *
 * @author Sergio Bossa
 */
public interface Node {

    /**
     * Connect to this node in order to be able send {@link terrastore.communication.protocol.Command}
     * messages to execute.
     */
    public void connect();

    /**
     * Send the given {@link terrastore.communication.protocol.Command} message, so that it can be locally or remotely executed.
     *
     * @param command The command to send.
     * @return The result of the executed command.
     * @throws CommunicationException If unable to communicate with the node.
     * @throws ProcessingException If an error occurs during command processing.
     */
    public <R> R send(Command<R> command) throws CommunicationException, ProcessingException;

    /**
     * Disconnect from this node.
     */
    public void disconnect();

    /**
     * Get this node name, unique among other nodes belonging to the same cluster.
     *
     * @return The node name.
     */
    public String getName();

    /**
     * Get this node host.
     *
     * @return The node host.
     */
    public String getHost();

    /**
     * Get this node port.
     *
     * @return The node port.
     */
    public int getPort();

    /**
     * Get the complete server configuration.
     *
     * @return The server configuration.
     */
    public ServerConfiguration getConfiguration();
}
