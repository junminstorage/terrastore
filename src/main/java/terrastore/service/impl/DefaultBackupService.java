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
package terrastore.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.backup.BackupException;
import terrastore.backup.BackupExporter;
import terrastore.backup.BackupImporter;
import terrastore.common.ErrorMessage;
import terrastore.communication.CommunicationException;
import terrastore.router.Router;
import terrastore.service.BackupOperationException;
import terrastore.service.BackupService;

/**
 * @author Sergio Bossa
 */
public class DefaultBackupService implements BackupService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBackupService.class);
    private final BackupImporter backupImporter;
    private final BackupExporter backupExporter;
    private final Router router;
    private final String secret;

    public DefaultBackupService(BackupImporter backupImporter, BackupExporter backupExporter, Router router, String secret) {
        this.backupImporter = backupImporter;
        this.backupExporter = backupExporter;
        this.router = router;
        this.secret = secret;
    }

    @Override
    public void importBackup(String bucket, String source, String secret) throws CommunicationException, BackupOperationException {
        try {
            if (secret.equals(this.secret)) {
                LOG.debug("Importing backup for bucket {} from {}", bucket, source);
                backupImporter.importBackup(router, bucket, source);
            } else {
                throw new BackupOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Bad secret: " + secret));
            }
        } catch (BackupException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new BackupOperationException(error);
        }
    }

    @Override
    public void exportBackup(String bucket, String destination, String secret) throws CommunicationException, BackupOperationException {
        try {
            if (secret.equals(this.secret)) {
                LOG.debug("Exporting backup for bucket {} to {}", bucket, destination);
                backupExporter.exportBackup(router, bucket, destination);
            } else {
                throw new BackupOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Bad secret: " + secret));
            }
        } catch (BackupException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new BackupOperationException(error);
        }
    }

    @Override
    public String getSecret() {
        return secret;
    }

    @Override
    public Router getRouter() {
        return router;
    }

    public BackupImporter getBackupImporter() {
        return backupImporter;
    }

    public BackupExporter getBackupExporter() {
        return backupExporter;
    }
}
