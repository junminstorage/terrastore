package terrastore.store.impl;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.store.BackupManager;
import terrastore.store.Bucket;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import terrastore.store.types.JsonValue;
import static terrastore.startup.Constants.*;

/**
 * @author Sergio Bossa
 */
public class DefaultBackupManager implements BackupManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBackupManager.class);
    //
    private final BiMap<Byte, Class> types = HashBiMap.create();
    private final Map<Class, ValueFactory> factories = new HashMap<Class, ValueFactory>();

    public DefaultBackupManager() {
        configureJsonValue();
    }

    @Override
    public void exportBackup(Bucket bucket, String destination) throws StoreOperationException {
        DataOutputStream dataStream = null;
        try {
            File resource = getResource(destination);
            dataStream = new DataOutputStream(new FileOutputStream(resource));
            for (String key : bucket.keys()) {
                Value value = bucket.get(key);
                if (value != null) {
                    byte[] content = value.getBytes();

                    dataStream.writeUTF(key);

                    dataStream.writeInt(content.length);
                    dataStream.write(content, 0, content.length);

                    dataStream.writeByte(types.inverse().get(value.getClass()));
                }
            }
        } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
        } finally {
            if (dataStream != null) {
                try {
                    dataStream.close();
                } catch (IOException ex) {
                    LOG.error(ex.getMessage(), ex);
                    throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
                }
            }
        }
    }

    @Override
    public void importBackup(Bucket bucket, String source) throws StoreOperationException {
        DataInputStream dataStream = null;
        try {
            File resource = getResource(source);
            dataStream = new DataInputStream(new FileInputStream(resource));
            while (true) {
                String key = dataStream.readUTF();

                int contentLength = dataStream.readInt();
                byte[] content = new byte[contentLength];
                dataStream.read(content, 0, contentLength);

                byte valueType = dataStream.readByte();

                Class valueClazz = types.get(valueType);
                ValueFactory valueFactory = factories.get(valueClazz);

                Value value = valueFactory.create(content);

                bucket.put(key, value);
            }
        } catch (EOFException ex) {
            LOG.info("EOF reached.");
        } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
        } finally {
            if (dataStream != null) {
                try {
                    dataStream.close();
                } catch (IOException ex) {
                    LOG.error(ex.getMessage(), ex);
                    throw new StoreOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
                }
            }
        }
    }

    private void configureJsonValue() {
        types.put(new Byte((byte) 1), JsonValue.class);
        factories.put(JsonValue.class, new ValueFactory() {

            @Override
            public Value create(byte[] bytes) {
                return new JsonValue(bytes);
            }
        });
    }

    private File getResource(String file) {
        String homeDir = System.getenv(TERRASTORE_HOME) != null ? System.getenv(TERRASTORE_HOME) : System.getProperty(TERRASTORE_HOME);
        if (homeDir != null) {
            String separator = System.getProperty("file.separator");
            return new File(homeDir + separator + BACKUPS_DIR + separator + file);
        } else {
            throw new IllegalStateException("Terrastore home directory is not set!");
        }
    }

    private static interface ValueFactory {

        public Value create(byte[] bytes);
    }
}
