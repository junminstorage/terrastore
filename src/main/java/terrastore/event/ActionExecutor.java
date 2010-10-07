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
package terrastore.event;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * Creates and executes {@link Action}s on the Terrastore cluster.
 *
 * @author Sergio Bossa
 */
public interface ActionExecutor {

    /**
     * Create an action to put a key/value pair in the given bucket.
     *
     * @param bucket
     * @param key
     * @param value
     * @return
     */
    public Action makePutAction(String bucket, String key, Map value);

    /**
     * Create an action to remove the document at the given key in the given bucket.
     *
     * @param bucket
     * @param key
     * @return
     */
    public Action makeRemoveAction(String bucket, String key);

    /**
     * Create an action to update the document at the given key in the given bucket.
     *
     * @param bucket
     * @param key
     * @param function
     * @param timeoutInMillis
     * @param parameters
     * @return
     */
    public Action makeUpdateAction(String bucket, String key, String function, long timeoutInMillis, Map parameters);

    /**
     * Submit the given action, returning a <em>Future</em> to eventually wait for action execution.
     * 
     * @param action
     * @return
     */
    public Future submit(Action action);
}
