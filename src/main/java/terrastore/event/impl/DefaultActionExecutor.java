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
import java.util.concurrent.Future;
import terrastore.event.Action;
import terrastore.event.ActionExecutor;
import terrastore.service.UpdateService;
import terrastore.util.global.GlobalExecutor;

/**
 * @author Sergio Bossa
 */
public class DefaultActionExecutor implements ActionExecutor {

    private final UpdateService updateService;

    public DefaultActionExecutor(UpdateService updateService) {
        this.updateService = updateService;
    }

    @Override
    public Action makePutAction(String bucket, String key, Map value) {
        return new PutAction(updateService, bucket, key, value);
    }

    @Override
    public Action makePutAction(String bucket, String key, Map value, String predicateExpression) {
        return new PutAction(updateService, bucket, key, value, predicateExpression);
    }

    @Override
    public Action makeRemoveAction(String bucket, String key) {
        return new RemoveAction(updateService, bucket, key);
    }

    @Override
    public Action makeUpdateAction(String bucket, String key, String function, long timeoutInMillis, Map parameters) {
        return new UpdateAction(updateService, bucket, key, function, timeoutInMillis, parameters);
    }

    @Override
    public Future submit(Action action) {
        return GlobalExecutor.getExecutor().submit(action);
    }
}
