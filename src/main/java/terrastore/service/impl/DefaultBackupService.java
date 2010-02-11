package terrastore.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
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
    public void importBackup(String bucket, String source, String secret) throws BackupOperationException {
        try {
            if (secret.equals(this.secret)) {
                LOG.info("Importing backup for bucket {} from {}", bucket, source);
                Node node = router.getLocalNode();
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
    public void exportBackup(String bucket, String destination, String secret) throws BackupOperationException {
        try {
            if (secret.equals(this.secret)) {
                LOG.info("Exporting backup for bucket {} to {}", bucket, destination);
                Node node = router.getLocalNode();
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
