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
package terrastore.store.features;

import java.io.Serializable;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Reducer object carrying data about the reducer function and its timeout.
 *
 * @author Sergio Bossa
 */
public class Reducer implements Serializable {

    private static final long serialVersionUID = 12345678901L;
    private final String reducerName;
    private final long timeoutInMillis;

    public Reducer(String reducerName, long timeoutInMillis) {
        this.reducerName = reducerName;
        this.timeoutInMillis = timeoutInMillis;
    }

    public String getReducerName() {
        return reducerName;
    }

    public long getTimeoutInMillis() {
        return timeoutInMillis;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Reducer) {
            Reducer other = (Reducer) obj;
            return new EqualsBuilder().append(this.reducerName, other.reducerName).
                    append(this.timeoutInMillis, other.timeoutInMillis).
                    isEquals();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(reducerName).
                append(timeoutInMillis).
                toHashCode();
    }

}
