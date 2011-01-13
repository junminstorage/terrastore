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
import java.util.Map;

/**
 * Represent an event related to a key/value pair under a specific bucket.
 *
 * @author Sergio Bossa
 */
public interface Event extends Serializable {

    /**
     * Get the unique id of this event.
     *
     * @return The unique id.
     */
    public String getId();

    /**
     * Get the name of the bucket containing the key/value entry
     * this event refers to.
     *
     * @return The bucket name.
     */
    public String getBucket();

    /**
     * Get the key this event refers to.
     *
     * @return The key.
     */
    public String getKey();

    /**
     * Get the old value this event refers to (if any).
     *
     * @return The value as byte array, or null if there's no old value.
     */
    public byte[] getOldValueAsBytes();

    /**
     * Get the old value this event refers to (if any).
     *
     * @return The value as map of primitive objects, or null if there's no old value.
     */
    public Map getOldValueAsMap();

    /**
     * Get the new value this event refers to (if any).
     *
     * @return The value as byte array, or null if there's no new value.
     */
    public byte[] getNewValueAsBytes();

    /**
     * Get the new value this event refers to (if any).
     *
     * @return The value as map of primitive objects, or null if there's no new value.
     */
    public Map getNewValueAsMap();

    /**
     * Dispatch this event to the given {@link EventListener} with the given {@link ActionExecutor}.
     *
     * @param listener The listener to dispatch this event to.
     * @param actionExecutor The executor to use for eventually submitting actions.
     */
    public void dispatch(EventListener listener, ActionExecutor actionExecutor);
}
