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

import terrastore.backup.BackupException;
import java.util.Map;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.backup.BackupExporter;
import terrastore.store.Bucket;
import terrastore.store.Key;
import terrastore.store.Value;
import static terrastore.startup.Constants.*;

/**
 * Default {@link terrastore.backup.BackupExporter} implementation, exporting
 * backups to files into the "backups" directory under Terrastore home
 * (see {@link terrastore.startup.Constants#TERRASTORE_HOME} and {@link terrastore.startup.Constants#BACKUPS_DIR}).
 *
 * @author Sergio Bossa
 */
public class DefaultBackupExporter implements BackupExporter {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBackupExporter.class);
    //
    private final Map<Class, Byte> types = new HashMap<Class, Byte>();

    public DefaultBackupExporter() {
        configureJsonValues();
    }

    @Override
    public void exportBackup(Bucket bucket, String destination) throws BackupException {
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
                    dataStream.writeByte(types.get(value.getClass()));
                }
            }
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
        types.put(Value.class, new Byte((byte) 1));
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

}
