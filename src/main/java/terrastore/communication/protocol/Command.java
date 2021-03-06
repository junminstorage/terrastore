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
package terrastore.communication.protocol;

import java.io.Serializable;
import terrastore.communication.CommunicationException;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;

/**
 * Command to be executed on a {@link terrastore.store.Store} instance.
 *
 * @author Sergio Bossa
 */
public interface Command<R> extends Serializable {

    /**
     * Execute this command on the given {@link terrastore.router.Router} instance.
     *
     * @param router The router to execute this command on.
     * @return The result of the executed command.
     * @throws CommunicationException If unable to communicate.
     * @throws MissingRouteException If no route is found for the command.
     * @throws ProcessingException If an error occurs during processing.
     */
    public Response<R> executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException;
    
    /**
     * Execute this command on the given {@link terrastore.store.Store} instance.
     *
     * @param store The store to execute this command on.
     * @return The result of the executed command.
     * @throws StoreOperationException If errors occur during command execution.
     */
    public Response<R> executeOn(Store store) throws StoreOperationException;

    public void setId(String id);

    public String getId();
}
