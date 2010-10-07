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

import terrastore.event.Action;
import terrastore.service.UpdateService;
import terrastore.store.Key;

/**
 * @author Sergio Bossa
 */
public class RemoveAction implements Action {

    private final UpdateService updateService;
    private final String bucket;
    private final String key;

    protected RemoveAction(UpdateService updateService, String bucket, String key) {
        this.updateService = updateService;
        this.bucket = bucket;
        this.key = key;
    }

    @Override
    public void execute() throws Exception {
        updateService.removeValue(bucket, new Key(key));
    }

    @Override
    public Object call() throws Exception {
        execute();
        return null;
    }
}
