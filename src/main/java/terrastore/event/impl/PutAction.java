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
package terrastore.event.impl;

import java.util.Map;
import terrastore.event.Action;
import terrastore.service.UpdateService;
import terrastore.store.Key;
import terrastore.store.features.Predicate;
import terrastore.util.json.JsonUtils;

/**
 * @author Sergio Bossa
 */
public class PutAction implements Action {

    private final UpdateService updateService;
    private final String bucket;
    private final String key;
    private final Map value;
    private final String predicateExpression;

    public PutAction(UpdateService updateService, String bucket, String key, Map value) {
        this(updateService, bucket, key, value, null);
    }

    public PutAction(UpdateService updateService, String bucket, String key, Map value, String predicateExpression) {
        this.updateService = updateService;
        this.bucket = bucket;
        this.key = key;
        this.value = value;
        this.predicateExpression = predicateExpression;
    }

    
    @Override
    public void execute() throws Exception {
        Predicate predicate = predicateExpression != null ? new Predicate(predicateExpression) : null;
        updateService.putValue(bucket, new Key(key), JsonUtils.fromMap(value), predicate);
    }

    @Override
    public Object call() throws Exception {
        execute();
        return null;
    }
}
