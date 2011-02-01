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
package terrastore.event;

import java.io.Serializable;

/**
 * Interface to implement for listening and reacting to bucket events.
 *
 * @author Sergio Bossa
 */
public interface EventListener extends Serializable {

    /**
     * Determine if this listener is interested into events happening in the given bucket.
     *
     * @param bucket The bucket to observe.
     * @return True if interested (observing) the given bucket, false otherwise.
     */
    public boolean observes(String bucket);

    /**
     * React when a given value changes.
     *
     * @param event The {@link Event} object carrying information about the current event.
     * @param executor The {@link ActionExecutor} for eventually creating and executing {@link Action}s.
     */
    public void onValueChanged(Event event, ActionExecutor executor);

    /**
     * React when a given value is removed.
     *
     * @param event The {@link Event} object carrying information about the current event.
     * @param executor The {@link ActionExecutor} for eventually creating and executing {@link Action}s.
     */
    public void onValueRemoved(Event event, ActionExecutor executor);

    /**
     * Callback to initialize things on listener registration.
     */
    public void init();

    /**
     * Callback to cleanup things on {@link EventBus} shutdown.
     */
    public void cleanup();
}
