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

import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Bucket;
import terrastore.store.Key;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import terrastore.store.features.Update;
import terrastore.store.operators.Function;

/**
 * @author Sergio Bossa
 */
public class UpdateCommand extends AbstractCommand<Value> {

    private final String bucketName;
    private final Key key;
    private final Update update;
    private final Function function;

    public UpdateCommand(String bucketName, Key key, Update update, Function function) {
        this.bucketName = bucketName;
        this.key = key;
        this.update = update;
        this.function = function;
    }

    @Override
    public Value executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException {
        Node node = router.routeToNodeFor(bucketName, key);
        return node.<Value>send(this);
    }

    public Value executeOn(Store store) throws StoreOperationException {
        Bucket bucket = store.getOrCreate(bucketName);
        if (bucket != null) {
            return bucket.update(key, update, function);
        } else {
            return null;
        }
    }
}
