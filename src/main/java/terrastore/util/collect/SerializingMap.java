package terrastore.util.collect;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class SerializingMap<K, V> extends AbstractMap<K, V> implements Serializable {

    private Map<K, V> source;

    public SerializingMap(Map<K, V> source) {
        this.source = source;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return source.entrySet();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        int size = source.size();
        out.writeInt(size);
        for (Entry<K, V> entry : source.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    private void readObject(ObjectInputStream in) throws IOException {
        try {
            int size = in.readInt();
            source = new LinkedHashMap<K, V>();
            for (int i = 0; i < size; i++) {
                source.put((K) in.readObject(), (V) in.readObject());
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
