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
 * Represent an event related to a key/value pair under a specific bucket.
 *
 * @author Sergio Bossa
 */
public interface Event {

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
     * Get the value this event refers to.
     *
     * @return The value.
     */
    public byte[] getValue();

    /**
     * Dispatch this event to the given {@link EventListener}s.
     *
     * @param listener The listener to dispatch this event to.
     */
    public void dispatch(EventListener listener);
}
