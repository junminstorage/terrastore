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

import java.util.List;
import java.util.Map;
import terrastore.communication.CommunicationException;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import terrastore.store.features.Reducer;

/**
 * @author Sergio Bossa
 */
public class ReduceCommand extends AbstractCommand<Value> {

    private final List<Map<String, Object>> values;
    private final Reducer reducer;

    public ReduceCommand(List<Map<String, Object>> values, Reducer reducer) {
        this.values = values;
        this.reducer = reducer;
    }

    @Override
    public Value executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException {
        return router.routeToLocalNode().<Value>send(this);
    }

    public Value executeOn(final Store store) throws StoreOperationException {
        return store.reduce(values, reducer);
    }

}
