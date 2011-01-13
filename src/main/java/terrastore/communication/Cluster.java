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
package terrastore.communication;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Cluster interface, representing an actual cluster in the ensemble.
 * <br><br>
 * Every cluster is actually comprised of a set of Terrastore server nodes<br>
 * All clusters makes up an ensemble.
 *
 * @author Sergio Bossa
 */
public class Cluster {

    private final String name;
    private final boolean local;

    public Cluster(String name, boolean local) {
        this.name = name;
        this.local = local;
    }

    public String getName() {
        return name;
    }

    public boolean isLocal() {
        return local;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Cluster) {
            Cluster other = (Cluster) obj;
            return new EqualsBuilder().append(this.name, other.name).isEquals();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(name).toHashCode();
    }

    @Override
    public String toString() {
        return name;
    }
}
