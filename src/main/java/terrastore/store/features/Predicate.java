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
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.msgpack.MessagePackable;
import org.msgpack.MessageTypeException;
import org.msgpack.MessageUnpackable;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.util.io.MsgPackUtils;

/**
 * Predicate object carrying data about the {@link terrastore.store.operators.Condition} to evaluate.<br>
 * It takes a predicate expression in the form: "type:expression", where "type" is the actual condition type,
 * and "expression" is the actual expression to evaluate (depending on the condition implementation).<br>
 * The predicate can be empty, meaning there's no condition to evaluate.
 *
 * @author Sergio Bossa
 */
public class Predicate implements MessagePackable, MessageUnpackable, Serializable {

    private static final long serialVersionUID = 12345678901L;
    //
    private boolean empty;
    private String conditionType;
    private String conditionExpression;

    public Predicate(String predicate) {
        if (predicate != null) {
            try {
                this.conditionType = predicate.substring(0, predicate.indexOf(":"));
                this.conditionExpression = predicate.substring(predicate.indexOf(":") + 1);
                this.empty = false;
            } catch (Exception ex) {
                throw new IllegalArgumentException("Wrong predicate format, should be type:expression, actually is: " + predicate);
            }
        } else {
            this.conditionType = null;
            this.conditionExpression = null;
            this.empty = true;
        }
    }

    public Predicate() {
    }

    public boolean isEmpty() {
        return empty;
    }

    public String getConditionType() {
        return conditionType;
    }

    public String getConditionExpression() {
        return conditionExpression;
    }

    @Override
    public void messagePack(Packer packer) throws IOException {
        MsgPackUtils.packBoolean(packer, empty);
        MsgPackUtils.packString(packer, conditionType);
        MsgPackUtils.packString(packer, conditionExpression);
    }

    @Override
    public void messageUnpack(Unpacker unpacker) throws IOException, MessageTypeException {
        empty = MsgPackUtils.unpackBoolean(unpacker);
        conditionType = MsgPackUtils.unpackString(unpacker);
        conditionExpression = MsgPackUtils.unpackString(unpacker);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Predicate) {
            Predicate other = (Predicate) obj;
            if (this.empty && other.empty) {
                return true;
            } else {
                return new EqualsBuilder().append(this.conditionExpression, other.conditionExpression).append(this.conditionType, other.conditionType).isEquals();
            }
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        if (empty) {
            return 0;
        } else {
            return new HashCodeBuilder().append(conditionExpression).append(conditionType).toHashCode();
        }
    }

}
