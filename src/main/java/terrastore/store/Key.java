package terrastore.store;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import org.terracotta.modules.annotations.InstrumentedClass;

/**
 * @author Sergio Bossa
 */
@InstrumentedClass
public class Key implements Comparable<Key>, Serializable {

    private static final long serialVersionUID = 12345678901L;
    private static final Charset CHARSET = Charset.forName("UTF-8");
    //
    private final byte[] bytes;

    public Key(String key) {
        this.bytes = key.getBytes(CHARSET);
    }

    public Key(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] toBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return new String(bytes, CHARSET);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof Key) {
            Key other = (Key) obj;
            return Arrays.equals(this.bytes, other.bytes);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public int compareTo(Key other) {
        return this.toString().compareTo(other.toString());
    }
}
