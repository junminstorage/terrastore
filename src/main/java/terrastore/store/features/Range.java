/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.store.features;

import java.io.Serializable;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Sergio Bossa
 */
public class Range implements Serializable {

    private static final long serialVersionUID = 12345678901L;

    private String startKey;
    private String endKey;
    private String keyComparatorName;
    private long timeToLive;

    public Range(String startKey, String endKey, String keyComparatorName, long timeToLive) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.keyComparatorName = keyComparatorName;
        this.timeToLive = timeToLive;
    }

    public String getStartKey() {
        return startKey;
    }

    public String getEndKey() {
        return endKey;
    }
    
    public String getKeyComparatorName() {
        return keyComparatorName;
    }

    public long getTimeToLive() {
        return timeToLive;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Range) {
            Range other = (Range) obj;
            return new EqualsBuilder()
                    .append(this.startKey, other.startKey)
                    .append(this.endKey, other.endKey)
                    .append(this.keyComparatorName, other.keyComparatorName)
                    .append(this.timeToLive, other.timeToLive)
                    .isEquals();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(startKey)
                .append(endKey)
                .append(keyComparatorName)
                .append(timeToLive)
                .toHashCode();
    }
}
