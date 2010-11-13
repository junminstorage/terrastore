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

import java.io.IOException;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.msgpack.MessagePackable;
import org.msgpack.MessageTypeException;
import org.msgpack.MessageUnpackable;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sergio Bossa
 */
public class ServerConfiguration implements MessagePackable, MessageUnpackable {

    private String name;
    private String nodeHost;
    private int nodePort;
    private String httpHost;
    private int httpPort;

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
    public void messagePack(Packer packer) throws IOException {
        MsgPackUtils.packString(packer, name);
        MsgPackUtils.packString(packer, nodeHost);
        MsgPackUtils.packInt(packer, nodePort);
        MsgPackUtils.packString(packer, httpHost);
        MsgPackUtils.packInt(packer, httpPort);
    }

    @Override
    public void messageUnpack(Unpacker unpacker) throws IOException, MessageTypeException {
        name = MsgPackUtils.unpackString(unpacker);
        nodeHost = MsgPackUtils.unpackString(unpacker);
        nodePort = MsgPackUtils.unpackInt(unpacker);
        httpHost = MsgPackUtils.unpackString(unpacker);
        httpPort = MsgPackUtils.unpackInt(unpacker);
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
