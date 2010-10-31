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
import java.util.Map;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Mapper object carrying data about the mapper and combiner functions, their timeout and parameters.
 *
 * @author Sergio Bossa
 */
public class Mapper implements Serializable {

    private static final long serialVersionUID = 12345678901L;
    private final String mapperName;
    private final String combinerName;
    private final long timeoutInMillis;
    private final Map<String, Object> parameters;

    public Mapper(String mapperName, String combinerName, long timeoutInMillis, Map<String, Object> parameters) {
        this.mapperName = mapperName;
        this.combinerName = combinerName;
        this.timeoutInMillis = timeoutInMillis;
        this.parameters = parameters;
    }

    public String getMapperName() {
        return mapperName;
    }

    public String getCombinerName() {
        return combinerName;
    }

    public long getTimeoutInMillis() {
        return timeoutInMillis;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Mapper) {
            Mapper other = (Mapper) obj;
            return new EqualsBuilder().append(this.mapperName, other.mapperName).
                    append(this.combinerName, other.combinerName).
                    append(this.timeoutInMillis, other.timeoutInMillis).
                    append(this.parameters, other.parameters).
                    isEquals();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(mapperName).
                append(combinerName).
                append(timeoutInMillis).
                append(parameters).
                toHashCode();
    }
}
