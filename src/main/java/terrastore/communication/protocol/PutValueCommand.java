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
import terrastore.store.Bucket;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.operators.Condition;

/**
 * @author Sergio Bossa
 */
public class PutValueCommand extends AbstractCommand {

    private final String bucketName;
    private final String key;
    private final Value value;
    private final boolean conditional;
    private final Predicate predicate;
    private final Condition valueCondition;

    public PutValueCommand(String bucketName, String key, Value value) {
        this.bucketName = bucketName;
        this.key = key;
        this.value = value;
        this.conditional = false;
        this.predicate = null;
        this.valueCondition = null;
    }

    public PutValueCommand(String bucketName, String key, Value value, Predicate predicate, Condition valueCondition) {
        this.bucketName = bucketName;
        this.key = key;
        this.value = value;
        this.conditional = true;
        this.predicate = predicate;
        this.valueCondition = valueCondition;
    }

    public Object executeOn(Store store) throws StoreOperationException {
        Bucket bucket = store.get(bucketName);
        if (bucket != null) {
            if (conditional) {
                boolean put = bucket.conditionalPut(key, value, predicate, valueCondition);
                if (!put) {
                    throw new StoreOperationException(new ErrorMessage(ErrorMessage.CONFLICT_ERROR_CODE,
                            "Unsatisfied condition: " + predicate.getConditionType() + ":" + predicate.getConditionExpression() + " for key: " + key));
                }
            } else {
                bucket.put(key, value);
            }
        }
        return null;
    }
}
