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
package terrastore.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.ExportBackupCommand;
import terrastore.communication.protocol.ImportBackupCommand;
import terrastore.router.Router;
import terrastore.service.BackupOperationException;
import terrastore.service.BackupService;

/**
 * @author Sergio Bossa
 */
public class DefaultBackupService implements BackupService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBackupService.class);
    private final Router router;
    private final String secret;

    public DefaultBackupService(Router router, String secret) {
        this.router = router;
        this.secret = secret;
    }

    @Override
    public void importBackup(String bucket, String source, String secret) throws CommunicationException, BackupOperationException {
        try {
            if (secret.equals(this.secret)) {
                LOG.info("Importing backup for bucket {} from {}", bucket, source);
                Node node = router.routeToLocalNode();
                ImportBackupCommand command = new ImportBackupCommand(bucket, source);
                node.send(command);
            } else {
                throw new BackupOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Bad secret: " + secret));
            }
        } catch (ProcessingException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new BackupOperationException(error);
        }
    }

    @Override
    public void exportBackup(String bucket, String destination, String secret) throws CommunicationException, BackupOperationException {
        try {
            if (secret.equals(this.secret)) {
                LOG.info("Exporting backup for bucket {} to {}", bucket, destination);
                Node node = router.routeToLocalNode();
                ExportBackupCommand command = new ExportBackupCommand(bucket, destination);
                node.send(command);
            } else {
                throw new BackupOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Bad secret: " + secret));
            }
        } catch (ProcessingException ex) {
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
}
