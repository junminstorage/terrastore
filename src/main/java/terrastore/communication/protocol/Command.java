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
package terrastore.communication.protocol;

import java.io.Serializable;
import java.util.Map;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;

/**
 * Command to be executed on a {@link terrastore.store.Store} instance.
 *
 * @author Sergio Bossa
 */
public interface Command extends Serializable {

    /**
     * Execute this command on the given {@link terrastore.store.Store} instance.
     *
     * @param store The store to execute this command on.
     * @return The result of the executed command, as a map of key/value pairs (if any). This never returns null.
     * @throws StoreOperationException If errors occur during command execution.
     */
    public Map<String, Value> executeOn(Store store) throws StoreOperationException;

    public void setId(String id);

    public String getId();
}
