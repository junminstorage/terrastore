/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.backup.impl;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.backup.BackupImporter;
import terrastore.backup.BackupException;
import terrastore.common.ErrorMessage;
import terrastore.communication.Node;
import terrastore.communication.protocol.PutValueCommand;
import terrastore.router.Router;
import terrastore.store.Key;
import terrastore.store.Value;
import static terrastore.startup.Constants.*;

/**
 * Default {@link terrastore.backup.BackupImporter} implementation, importing
 * backups from provided source files.
 *
 * @author Sergio Bossa
 */
public class DefaultBackupImporter implements BackupImporter {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBackupImporter.class);
    //
    private final Map<Byte, Class> types = new HashMap<Byte, Class>();
    private final Map<Class, ValueFactory> factories = new HashMap<Class, ValueFactory>();

    public DefaultBackupImporter() {
        configureJsonValues();
    }

    @Override
    public void importBackup(Router router, String bucket, String source) throws BackupException {
        DataInputStream dataStream = null;
        try {
            File resource = getResource(source);
            LOG.debug("Importing bucket {} from {}", bucket, resource.getAbsolutePath());
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
                Node node = router.routeToNodeFor(bucket, key);
                node.send(new PutValueCommand(bucket, key, value));
            }
        } catch (EOFException ex) {
            LOG.info("EOF reached.");
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new BackupException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
        } finally {
            if (dataStream != null) {
                try {
                    dataStream.close();
                } catch (IOException ex) {
                    LOG.error(ex.getMessage(), ex);
                    throw new BackupException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
                }
            }
        }
    }

    private void configureJsonValues() {
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
