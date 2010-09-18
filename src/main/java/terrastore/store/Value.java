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
package terrastore.store;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import terrastore.store.features.Predicate;
import terrastore.store.features.Update;
import terrastore.store.operators.Condition;
import terrastore.store.operators.Function;
import terrastore.util.json.JsonUtils;

/**
 * Json value object contained by {@link Bucket} instances.
 *
 * @author Sergio Bossa
 */
public class Value implements Serializable {

    private static final long serialVersionUID = 12345678901L;
    private static final Charset CHARSET = Charset.forName("UTF-8");
    //
    private final byte[] bytes;

    public Value(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public Value dispatch(Key key, Update update, Function function) {
        return JsonUtils.fromMap(function.apply(key.toString(), JsonUtils.toModifiableMap(this), update.getParameters()));
    }

    public boolean dispatch(Key key, Predicate predicate, Condition condition) {
        return condition.isSatisfied(key.toString(), JsonUtils.toUnmodifiableMap(this), predicate.getConditionExpression());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Value) {
            Value other = (Value) obj;
            return Arrays.equals(other.bytes, this.bytes);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }

    @Override
    public String toString() {
        return new String(bytes, CHARSET);
    }
}
