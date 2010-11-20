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

import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.msgpack.MessagePackable;
import org.msgpack.MessageTypeException;
import org.msgpack.MessageUnpackable;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.cluster.coordinator.ServerConfiguration;
import terrastore.util.io.MsgPackUtils;

/**
 * @author Sergio Bossa
 * @author Amir Moulavi
 */
public class View implements MessagePackable, MessageUnpackable, Serializable {

    private static final long serialVersionUID = 12345678901L;
    //
    private String cluster;
    private Set<Member> members;

    public View(String cluster, Set<Member> members) {
        this.cluster = cluster;
        this.members = members;
    }

    public View() {
    }

    public String getCluster() {
        return cluster;
    }

    public Set<Member> getMembers() {
        return members;
    }

    public int percentageOfChange(View anotherView) {
        Set<Member> a = new HashSet<Member>(members);
        Set<Member> b = new HashSet<Member>(anotherView.getMembers());
        int abDifference = Sets.difference(a, b).size();
        int baDifference = Sets.difference(b, a).size();
        int abUnion = Sets.union(a, b).size();
        return (int) (((float) (abDifference + baDifference) / abUnion) * 100);
    }

    @Override
    public void messagePack(Packer packer) throws IOException {
        MsgPackUtils.packString(packer, cluster);
        MsgPackUtils.packInt(packer, members.size());
        for (Member member : members) {
            MsgPackUtils.packServerConfiguration(packer, member.configuration);
        }
    }

    @Override
    public void messageUnpack(Unpacker unpacker) throws IOException, MessageTypeException {
        cluster = MsgPackUtils.unpackString(unpacker);
        members = new LinkedHashSet<Member>();
        int size = MsgPackUtils.unpackInt(unpacker);
        for (int i = 0; i < size; i++) {
            members.add(new Member(MsgPackUtils.unpackServerConfiguration(unpacker)));
        }
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
        private final ServerConfiguration configuration;

        public Member(ServerConfiguration configuration) {
            this.configuration = configuration;
        }

        public ServerConfiguration getConfiguration() {
            return configuration;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Member) {
                Member other = (Member) obj;
                return new EqualsBuilder().append(this.configuration.getName(), other.configuration.getName()).isEquals();
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(this.configuration.getName()).toHashCode();
        }

    }
}
