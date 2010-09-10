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
