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
package terrastore.event.impl;

import java.util.Map;
import terrastore.event.Action;
import terrastore.service.UpdateService;
import terrastore.store.Key;
import terrastore.store.features.Update;

/**
 * @author Sergio Bossa
 */
public class UpdateAction implements Action {

    private final UpdateService updateService;
    private final String bucket;
    private final String key;
    private final String function;
    private final long timeoutInMillis;
    private final Map parameters;

    protected UpdateAction(UpdateService updateService, String bucket, String key, String function, long timeoutInMillis, Map parameters) {
        this.updateService = updateService;
        this.bucket = bucket;
        this.key = key;
        this.function = function;
        this.timeoutInMillis = timeoutInMillis;
        this.parameters = parameters;
    }

    @Override
    public void execute() throws Exception {
        updateService.updateValue(bucket, new Key(key), new Update(function, timeoutInMillis, parameters));
    }

    @Override
    public Object call() throws Exception {
        execute();
        return null;
    }
}
