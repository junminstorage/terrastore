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

/**
 * Interface for dispatching an {@link Event} to all registered
 * {@link EventListener}s.
 *
 * @author Sergio Bossa
 */
public interface EventDispatcher {

    /**
     * The {@link Event} to dispatch.
     *
     * @return The {@link Event}.
     */
    public Event getEvent();

    /**
     * Register an {@link EventListener}.
     *
     * @param listener The {@link EventListener}.
     */
    public void addEventListener(EventListener listener);

    /**
     * @return True if there are registered {@link EventListener}s, false otherwise.
     */
    public boolean hasListeners();

    /**
     * Dispatch the {@link Event} to all registered
     * {@link EventListener}s.
     */
    public void dispatch();
}
