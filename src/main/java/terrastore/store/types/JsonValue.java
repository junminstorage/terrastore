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
package terrastore.store.types;

import terrastore.store.*;
import java.util.Arrays;
import org.terracotta.modules.annotations.InstrumentedClass;
import terrastore.store.features.Predicate;
import terrastore.store.features.Update;
import terrastore.store.operators.Condition;
import terrastore.store.operators.Function;
import terrastore.util.json.JsonUtils;

/**
 * Generic value object contained by {@link Bucket} instances.
 *
 * @author Sergio Bossa
 */
@InstrumentedClass
public class JsonValue implements Value {

    private static final long serialVersionUID = 12345678901L;

    private final byte[] bytes;

    public JsonValue(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public Value dispatch(String key, Update update, Function function) {
        return JsonUtils.fromMap(function.apply(key, JsonUtils.toModifiableMap(this), update.getParameters()));
    }

    @Override
    public boolean dispatch(String key, Predicate predicate, Condition condition) {
        return condition.isSatisfied(key, JsonUtils.toUnmodifiableMap(this), predicate.getConditionExpression());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof JsonValue) {
            JsonValue other = (JsonValue) obj;
            return Arrays.equals(other.bytes, this.bytes);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }
}
