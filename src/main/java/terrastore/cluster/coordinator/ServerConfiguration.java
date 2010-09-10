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
package terrastore.cluster.coordinator;

import java.io.Serializable;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Sergio Bossa
 */
public class ServerConfiguration implements Serializable {

    private static final long serialVersionUID = 12345678901L;
    //
    private final String name;
    private final String nodeHost;
    private final int nodePort;
    private final String httpHost;
    private final int httpPort;

    public ServerConfiguration(String name, String nodeHost, int nodePort, String httpHost, int httpPort) {
        this.name = name;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.httpHost = httpHost;
        this.httpPort = httpPort;
    }

    public String getName() {
        return name;
    }

    public String getHttpHost() {
        return httpHost;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public String getNodeHost() {
        return nodeHost;
    }

    public int getNodePort() {
        return nodePort;
    }

    @Override
        public boolean equals(Object obj) {
            if (obj instanceof ServerConfiguration) {
                ServerConfiguration other = (ServerConfiguration) obj;
                return new EqualsBuilder().append(this.name, other.name).isEquals();
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(this.name).toHashCode();
        }
}
