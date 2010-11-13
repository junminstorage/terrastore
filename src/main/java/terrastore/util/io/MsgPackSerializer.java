package terrastore.util.io;

import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Sergio Bossa
 */
public class MsgPackSerializer<T> implements Serializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(MsgPackSerializer.class);

    @Override
    public byte[] serialize(T object) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        LZFOutputStream stream = new LZFOutputStream(bytes);
        try {
            Packer packer = new Packer(stream);
            packer.packString(object.getClass().getName());
            packer.pack(object);
            stream.flush();
            return bytes.toByteArray();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            try {
                stream.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
    }

    @Override
    public T deserialize(byte[] serialized) {
        return deserialize(new ByteArrayInputStream(serialized));
    }

    @Override
    public T deserialize(InputStream serialized) {
        LZFInputStream stream = null;
        try {
            stream = new LZFInputStream(serialized);
            Unpacker unpacker = new Unpacker(stream);
            String className = unpacker.unpackString();
            return unpacker.unpack((Class<T>) Class.forName(className));
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            try {
                stream.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
    }

}
