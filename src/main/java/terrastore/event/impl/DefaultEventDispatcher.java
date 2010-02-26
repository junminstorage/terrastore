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

import terrastore.event.EventDispatcher;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.event.Event;
import terrastore.event.EventListener;

/**
 * @author Sergio Bossa
 */
public class DefaultEventDispatcher implements EventDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultEventDispatcher.class);
    //
    private final Event event;
    private final List<EventListener> listeners = new LinkedList<EventListener>();

    public DefaultEventDispatcher(Event event) {
        this.event = event;
    }

    @Override
    public Event getEvent() {
        return event;
    }

    public void addEventListener(EventListener listener) {
        listeners.add(listener);
    }

    public boolean hasListeners() {
        return listeners.size() > 0;
    }

    public void dispatch() {
        for (EventListener listener : listeners) {
            try {
                event.dispatch(listener);
            } catch (Exception ex) {
                // TODO: improve error handling!
                LOG.warn("Failed listener: " + listener.toString());
                LOG.warn(ex.getMessage(), ex);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DefaultEventDispatcher) {
            DefaultEventDispatcher other = (DefaultEventDispatcher) obj;
            return new EqualsBuilder().append(this.event, other.event).append(this.listeners, other.listeners).isEquals();
        } else {
            return false;
        }
    }


    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.event).append(this.listeners).toHashCode();
    }
}
