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

import java.io.Serializable;

/**
 * Observe buckets and react to events related to key/value pairs.
 *
 * @author Sergio Bossa
 */
public interface EventListener extends Serializable {

    /**
     * Determine if this listener is interested to events happening in the given bucket.
     *
     * @param bucket The bucket to observe.
     * @return True if interested (observing) the given bucket, false otherwise.
     */
    public boolean observes(String bucket);

    /**
     * React when a given value changes.
     *
     * @param bucket Name of the bucket containing the changed value.
     * @param key The key of the changing value.
     * @param value The changed value.
     */
    public void onValueChanged(String bucket, String key, byte[] value);

    /**
     * React when a given value is removed.
     *
     * @param bucket Name of the bucket containing the removed value.
     * @param key The key of the removed value.
     */
    public void onValueRemoved(String bucket, String key);

    /**
     * Callback to initialize things on listener registration.
     */
    public void init();

    /**
     * Callback to cleanup things on {@link EventBus} shutdown.
     */
    public void cleanup();
}
