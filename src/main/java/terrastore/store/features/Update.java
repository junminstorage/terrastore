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
 * Update object carrying data about the update function, timeout and parameters.
 *
 * @author Sergio Bossa
 */
public class Update implements Serializable {

    private static final long serialVersionUID = 12345678901L;
    private final String functionName;
    private final long timeoutInMillis;
    private final Map<String, Object> parameters;

    public Update(String functionName, long timeoutInMillis, Map<String, Object> parameters) {
        this.functionName = functionName;
        this.timeoutInMillis = timeoutInMillis;
        this.parameters = parameters;
    }

    public long getTimeoutInMillis() {
        return timeoutInMillis;
    }

    public String getFunctionName() {
        return functionName;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Update) {
            Update other = (Update) obj;
            return new EqualsBuilder().append(this.functionName, other.functionName).append(this.timeoutInMillis, other.timeoutInMillis).append(this.parameters, other.parameters).
                    isEquals();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(functionName).append(timeoutInMillis).append(parameters).toHashCode();
    }
}
