package terrastore.service;

import terrastore.router.Router;

/**
 * The BackupService manages the operations of exporting and importing bucket entries.<br>
 * Import/export are performed from/to a resource and the actual implementation is delegated to the underlying
 * store.
 *
 * @author Sergio Bossa
 */
public interface BackupService {

   /**
     * Import all bucket key/value entries, without interrupting other operations and preserving
     * existent entries not contained into the given backup.
     *
     * @param bucket The bucket to import entries to.
     * @param source The name of the resource from which reading the backup.
     * @param secret The secret key: import is executed only if it matches the pre-configured secret.
     * @throws BackupOperationException If an error occurs.
     */
    public void importBackup(String bucket, String source, String secret) throws BackupOperationException;

    /**
     * Export all bucket key/value entries, without interrupting other operations.
     *
     * @param bucket The bucket to export entries from.
     * @param destination The name of the resource to which writing the backup.
     * @param secret The secret key: export is executed only if it matches the pre-configured secret.
     * @throws BackupOperationException If an error occurs.
     */
    public void exportBackup(String bucket, String destination, String secret) throws BackupOperationException;

    /**
     * The pre-configured secret that import/export operation has to match.
     *
     * @return The secret key.
     */
    public String getSecret();

    /**
     * Get the {@link terrastore.router.Router} instance used for routing actual update operations.
     *
     * @return The router instance.
     */
    public Router getRouter();
}
