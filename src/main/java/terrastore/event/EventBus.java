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

import java.util.List;

/**
 * Interface for publishing {@link Event}s to be processed by registered {@link EventListener}s.
 *
 * @author Sergio Bossa
 */
public interface EventBus {

    /**
     * Get the {@link ActionExecutor} used to execute actions on listeners.
     *
     * @return The action executor.
     */
    public ActionExecutor getActionExecutor();

    /**
     * Get the list or registered {@link EventListener}s.
     *
     * @return The list of listeners.
     */
    public List<EventListener> getEventListeners();

    /**
     * Publish an {@link Event}.
     *
     * @param event The event to publish.
     */
    public void publish(Event event);

    /**
     * Stop events publishing and processing.
     */
    public void shutdown();
}
