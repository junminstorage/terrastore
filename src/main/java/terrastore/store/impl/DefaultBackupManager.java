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
package terrastore.store.impl;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
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
import terrastore.store.Key;
import terrastore.store.StoreOperationException;
import terrastore.store.Value;
import static terrastore.startup.Constants.*;

/**
 * Default {@link terrastore.store.BackupManager} implementation, exporting/importing
 * backups to/from files into the "backups" directory under Terrastore home
 * (see {@link terrastore.startup.Constants#TERRASTORE_HOME} and {@link terrastore.startup.Constants#BACKUPS_DIR}).
 *
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
            LOG.debug("Exporting bucket {} to {}", bucket.getName(), resource.getAbsolutePath());
            dataStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(resource)));
            for (Key key : bucket.keys()) {
                Value value = bucket.get(key);
                if (value != null) {
                    byte[] content = value.getBytes();
                    // Write key:
                    dataStream.writeUTF(key.toString());
                    // Write value:
                    dataStream.writeInt(content.length);
                    dataStream.write(content, 0, content.length);
                    // Write value type:
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
            LOG.debug("Importing bucket {} from {}", bucket.getName(), resource.getAbsolutePath());
            dataStream = new DataInputStream(new BufferedInputStream(new FileInputStream(resource)));
            while (true) {
                // Read key:
                Key key = new Key(dataStream.readUTF());
                // Read value:
                int contentLength = dataStream.readInt();
                byte[] content = new byte[contentLength];
                dataStream.read(content, 0, contentLength);
                // Read type:
                byte valueType = dataStream.readByte();
                // Create value:
                Class valueClazz = types.get(valueType);
                ValueFactory valueFactory = factories.get(valueClazz);
                Value value = valueFactory.create(content);
                // Put:
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
        types.put(new Byte((byte) 1), Value.class);
        factories.put(Value.class, new ValueFactory() {

            @Override
            public Value create(byte[] bytes) {
                return new Value(bytes);
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
