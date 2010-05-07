package terrastore.util.collect;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class SerializingSet<E> extends AbstractSet<E> implements Serializable {

    private Set<E> source;

    public SerializingSet(Set<E> source) {
        this.source = source;
    }

    @Override
    public Iterator<E> iterator() {
        return source.iterator();
    }

    @Override
    public int size() {
        return source.size();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        int size = source.size();
        out.writeInt(size);
        for (E element : source) {
            out.writeObject(element);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException {
        try {
            int size = in.readInt();
            source = new LinkedHashSet<E>();
            for (int i = 0; i < size; i++) {
                source.add((E) in.readObject());
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
