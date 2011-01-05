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
package terrastore.util.io;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import terrastore.communication.NodeConfiguration;
import terrastore.cluster.ensemble.impl.View;
import terrastore.common.ErrorMessage;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.store.features.Mapper;
import terrastore.store.features.Predicate;
import terrastore.store.features.Range;
import terrastore.store.features.Reducer;
import terrastore.store.features.Update;

/**
 * @author Sergio Bossa
 */
public class MsgPackUtils {

    private static final JavaSerializer HELPER = new JavaSerializer();

    public static void packBoolean(Packer packer, boolean value) throws IOException {
        packer.packBoolean(value);
    }

    public static void packBytes(Packer packer, byte[] value) throws IOException {
        packer.packByteArray(value);
    }

    public static void packInt(Packer packer, int value) throws IOException {
        packer.packInt(value);
    }

    public static void packLong(Packer packer, long value) throws IOException {
        packer.packLong(value);
    }

    public static void packString(Packer packer, String value) throws IOException {
        packer.packString(value);
    }

    public static void packKey(Packer packer, Key key) throws IOException {
        if (key != null) {
            packer.pack(key);
        } else {
            packer.packNil();
        }
    }

    public static void packValue(Packer packer, Value value) throws IOException {
        if (value != null) {
            packer.pack(value);
        } else {
            packer.packNil();
        }
    }

    public static void packErrorMessage(Packer packer, ErrorMessage errorMessage) throws IOException {
        if (errorMessage != null) {
            packer.pack(errorMessage);
        } else {
            packer.packNil();
        }
    }

    public static void packKeys(Packer packer, Set<Key> keys) throws IOException {
        if (keys != null) {
            packer.packInt(keys.size());
            for (Key key : keys) {
                packKey(packer, key);
            }
        } else {
            packer.packNil();
        }
    }

    public static void packValues(Packer packer, Map<Key, Value> values) throws IOException {
        if (values != null) {
            packer.packInt(values.size());
            for (Map.Entry<Key, Value> entry : values.entrySet()) {
                packKey(packer, entry.getKey());
                packValue(packer, entry.getValue());
            }
        } else {
            packer.packNil();
        }
    }

    public static void packGenericMap(Packer packer, Map<String, Object> genericMap) throws IOException {
        if (genericMap != null) {
            packer.pack(HELPER.serialize(genericMap));
        } else {
            packer.packNil();
        }
    }

    public static void packGenericSet(Packer packer, Set genericSet) throws IOException {
        if (genericSet != null) {
            packer.pack(HELPER.serialize(genericSet));
        } else {
            packer.packNil();
        }
    }

    public static void packMapper(Packer packer, Mapper mapper) throws IOException {
        if (mapper != null) {
            packer.pack(mapper);
        } else {
            packer.packNil();
        }
    }

    public static void packReducer(Packer packer, Reducer reducer) throws IOException {
        if (reducer != null) {
            packer.pack(reducer);
        } else {
            packer.packNil();
        }
    }

    public static void packPredicate(Packer packer, Predicate predicate) throws IOException {
        if (predicate != null) {
            packer.pack(predicate);
        } else {
            packer.packNil();
        }
    }

    public static void packRange(Packer packer, Range range) throws IOException {
        if (range != null) {
            packer.pack(range);
        } else {
            packer.packNil();
        }
    }

    public static void packUpdate(Packer packer, Update update) throws IOException {
        if (update != null) {
            packer.pack(update);
        } else {
            packer.packNil();
        }
    }

    public static void packServerConfiguration(Packer packer, NodeConfiguration serverConfiguration) throws IOException {
        if (serverConfiguration != null) {
            packer.pack(serverConfiguration);
        } else {
            packer.packNil();
        }
    }

    public static void packView(Packer packer, View view) throws IOException {
        if (view != null) {
            packer.pack(view);
        } else {
            packer.packNil();
        }
    }

    public static boolean unpackBoolean(Unpacker unpacker) throws IOException {
        return unpacker.unpackBoolean();
    }

    public static byte[] unpackBytes(Unpacker unpacker) throws IOException {
        return unpacker.unpackByteArray();
    }

    public static int unpackInt(Unpacker unpacker) throws IOException {
        return unpacker.unpackInt();
    }

    public static long unpackLong(Unpacker unpacker) throws IOException {
        return unpacker.unpackLong();
    }

    public static String unpackString(Unpacker unpacker) throws IOException {
        return unpacker.unpackString();
    }

    public static Key unpackKey(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            return unpacker.unpack(Key.class);
        }
    }

    public static Value unpackValue(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            return unpacker.unpack(Value.class);
        }
    }

    public static ErrorMessage unpackErrorMessage(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            return unpacker.unpack(ErrorMessage.class);
        }
    }

    public static Set<Key> unpackKeys(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            int size = unpackInt(unpacker);
            Set<Key> keys = new LinkedHashSet<Key>();
            for (int i = 0; i < size; i++) {
                keys.add(unpackKey(unpacker));
            }
            return keys;
        }
    }

    public static Map<Key, Value> unpackValues(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            int size = unpackInt(unpacker);
            Map<Key, Value> values = new LinkedHashMap<Key, Value>();
            for (int i = 0; i < size; i++) {
                values.put(unpackKey(unpacker), unpackValue(unpacker));
            }
            return values;
        }
    }

    public static Map<String, Object> unpackGenericMap(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            return (Map<String, Object>) HELPER.deserialize(unpacker.unpackByteArray());
        }
    }

    public static Set unpackGenericSet(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            return (Set) HELPER.deserialize(unpacker.unpackByteArray());
        }
    }

    public static Mapper unpackMapper(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            return unpacker.unpack(Mapper.class);
        }
    }

    public static Reducer unpackReducer(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            return unpacker.unpack(Reducer.class);
        }
    }

    public static Predicate unpackPredicate(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            return unpacker.unpack(Predicate.class);
        }
    }

    public static Range unpackRange(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            return unpacker.unpack(Range.class);
        }
    }

    public static Update unpackUpdate(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            return unpacker.unpack(Update.class);
        }
    }

    public static NodeConfiguration unpackServerConfiguration(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            return unpacker.unpack(NodeConfiguration.class);
        }
    }

    public static View unpackView(Unpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNull()) {
            return null;
        } else {
            return unpacker.unpack(View.class);
        }
    }
}
