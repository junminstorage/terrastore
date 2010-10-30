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
import java.util.UUID;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import terrastore.event.Event;
import terrastore.util.json.JsonUtils;

/**
 * @author Sergio Bossa
 */
public abstract class AbstractEvent implements Event {

    private final String id;
    private final String bucket;
    private final String key;
    private final byte[] oldValue;
    private final byte[] newValue;

    public AbstractEvent(String bucket, String key, byte[] oldValue, byte[] newValue) {
        this.id = UUID.randomUUID().toString();
        this.bucket = bucket;
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getBucket() {
        return bucket;
    }

    @Override
    public final String getKey() {
        return key;
    }

    @Override
    public final byte[] getOldValueAsBytes() {
        return oldValue;
    }

    @Override
    public Map getOldValueAsMap() {
        return JsonUtils.toUnmodifiableMap(oldValue);
    }

    @Override
    public final byte[] getNewValueAsBytes() {
        return newValue;
    }

    @Override
    public Map getNewValueAsMap() {
        return JsonUtils.toUnmodifiableMap(newValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AbstractEvent) {
            AbstractEvent other = (AbstractEvent) obj;
            return new EqualsBuilder().append(this.id, other.id).isEquals();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.id).toHashCode();
    }
}
