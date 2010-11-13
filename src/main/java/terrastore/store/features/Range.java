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

import java.io.IOException;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.msgpack.MessagePackable;
import org.msgpack.MessageTypeException;
import org.msgpack.MessageUnpackable;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.store.Key;
import terrastore.util.io.MsgPackUtils;

/**
 * Range object carrying data about range queries.
 *
 * @author Sergio Bossa
 */
public class Range implements MessagePackable, MessageUnpackable {

    private Key startKey;
    private Key endKey;
    private int limit;
    private String keyComparatorName;
    private long timeToLive;

    public Range(Key startKey, Key endKey, int limit, String keyComparatorName, long timeToLive) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.limit = limit;
        this.keyComparatorName = keyComparatorName;
        this.timeToLive = timeToLive;
    }

    public Range() {
    }

    public boolean isEmpty() {
        return startKey == null || startKey.toString().isEmpty();
    }

    public Key getStartKey() {
        return startKey;
    }

    public Key getEndKey() {
        return endKey;
    }

    public int getLimit() {
        return limit;
    }

    public String getKeyComparatorName() {
        return keyComparatorName;
    }

    public long getTimeToLive() {
        return timeToLive;
    }

    @Override
    public void messagePack(Packer packer) throws IOException {
        MsgPackUtils.packKey(packer, startKey);
        MsgPackUtils.packKey(packer, endKey);
        MsgPackUtils.packInt(packer, limit);
        MsgPackUtils.packString(packer, keyComparatorName);
        MsgPackUtils.packLong(packer, timeToLive);
    }

    @Override
    public void messageUnpack(Unpacker unpacker) throws IOException, MessageTypeException {
        startKey = MsgPackUtils.unpackKey(unpacker);
        endKey = MsgPackUtils.unpackKey(unpacker);
        limit = MsgPackUtils.unpackInt(unpacker);
        keyComparatorName = MsgPackUtils.unpackString(unpacker);
        timeToLive = MsgPackUtils.unpackLong(unpacker);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Range) {
            Range other = (Range) obj;
            return new EqualsBuilder().append(this.startKey, other.startKey).
                    append(this.endKey, other.endKey).
                    append(this.keyComparatorName, other.keyComparatorName).
                    append(this.timeToLive, timeToLive).
                    isEquals();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(startKey).
                append(endKey).
                append(keyComparatorName).
                append(timeToLive).
                toHashCode();
    }

}
