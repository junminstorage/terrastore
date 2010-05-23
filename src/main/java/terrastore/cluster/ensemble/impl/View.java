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
package terrastore.cluster.ensemble.impl;

import java.io.Serializable;
import java.util.Set;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Sergio Bossa
 */
public class View implements Serializable {

    private static final long serialVersionUID = 12345678901L;
    //
    private final String cluster;
    private final Set<Member> members;

    public View(String cluster, Set<Member> members) {
        this.cluster = cluster;
        this.members = members;
    }

    public String getCluster() {
        return cluster;
    }

    public Set<Member> getMembers() {
        return members;
    }

    @Override
        public boolean equals(Object obj) {
            if (obj instanceof View) {
                View other = (View) obj;
                return new EqualsBuilder().append(this.cluster, other.cluster).append(this.members, other.members).isEquals();
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(this.cluster).append(this.members).toHashCode();
        }

    public static class Member implements Serializable {

        private static final long serialVersionUID = 12345678901L;
        //
        private final String name;
        private final String host;
        private final int port;

        public Member(String name, String host, int port) {
            this.name = name;
            this.host = host;
            this.port = port;
        }

        public String getName() {
            return name;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Member) {
                Member other = (Member) obj;
                return new EqualsBuilder().append(this.name, other.name).append(this.host, other.host).append(this.port, other.port).isEquals();
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(this.name).append(this.host).append(this.port).toHashCode();
        }
    }
}
