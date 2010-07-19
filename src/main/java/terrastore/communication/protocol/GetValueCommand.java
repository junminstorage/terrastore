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

import terrastore.common.ErrorMessage;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Bucket;
import terrastore.store.Key;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.operators.Condition;

/**
 * @author Sergio Bossa
 */
public class GetValueCommand extends AbstractCommand<Value> {

    private final String bucketName;
    private final Key key;
    private final boolean conditional;
    private final Predicate predicate;
    private final Condition condition;

    public GetValueCommand(String bucketName, Key key) {
        this.bucketName = bucketName;
        this.key = key;
        this.conditional = false;
        this.predicate = null;
        this.condition = null;
    }

    public GetValueCommand(String bucketName, Key key, Predicate predicate, Condition condition) {
        this.bucketName = bucketName;
        this.key = key;
        this.conditional = true;
        this.predicate = predicate;
        this.condition = condition;
    }

    @Override
    public Value executeOn(Router router) throws MissingRouteException, ProcessingException {
        Node node = router.routeToNodeFor(bucketName, key);
        return node.<Value>send(this);
    }

    public Value executeOn(Store store) throws StoreOperationException {
        Bucket bucket = store.get(bucketName);
        if (bucket != null) {
            if (conditional) {
                Value value = bucket.conditionalGet(key, predicate, condition);
                if (value != null) {
                    return value;
                } else {
                    throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE,
                            "Unsatisfied condition: " + predicate.getConditionType() + ":" + predicate.getConditionExpression() + " for key: " + key));
                }
            } else {
                return bucket.get(key);
            }
        } else {
            // Deal with non existent bucket as if it were a non existent key:
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.NOT_FOUND_ERROR_CODE, "Key not found: " + key));
        }
    }
}
