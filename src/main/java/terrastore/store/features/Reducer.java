/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
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

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.msgpack.MessagePackable;
import org.msgpack.MessageTypeException;
import org.msgpack.MessageUnpackable;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.util.io.MsgPackUtils;

/**
 * Reducer object carrying data about the reducer function and its timeout.
 *
 * @author Sergio Bossa
 */
public class Reducer implements MessagePackable, MessageUnpackable, Serializable {

    private static final long serialVersionUID = 12345678901L;
    //
    private String reducerName;
    private long timeoutInMillis;
    private Map<String, Object> parameters;

    public Reducer(String reducerName, long timeoutInMillis, Map<String, Object> parameters) {
        this.reducerName = reducerName;
        this.timeoutInMillis = timeoutInMillis;
        this.parameters = parameters;
    }

    public Reducer() {
    }

    public String getReducerName() {
        return reducerName;
    }

    public long getTimeoutInMillis() {
        return timeoutInMillis;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    @Override
    public void messagePack(Packer packer) throws IOException {
        MsgPackUtils.packString(packer, reducerName);
        MsgPackUtils.packLong(packer, timeoutInMillis);
        MsgPackUtils.packGenericMap(packer, parameters);
    }

    @Override
    public void messageUnpack(Unpacker unpacker) throws IOException, MessageTypeException {
        reducerName = MsgPackUtils.unpackString(unpacker);
        timeoutInMillis = MsgPackUtils.unpackLong(unpacker);
        parameters = MsgPackUtils.unpackGenericMap(unpacker);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Reducer) {
            Reducer other = (Reducer) obj;
            return new EqualsBuilder().append(this.reducerName, other.reducerName).
                    append(this.timeoutInMillis, other.timeoutInMillis).append(this.parameters, other.parameters).
                    isEquals();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(reducerName).
                append(timeoutInMillis).append(parameters).
                toHashCode();
    }

}
