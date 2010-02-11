package terrastore.store;

/**
 * Execute export/import of {@link Bucket} entries to/from a given resource.<br>
 *
 * @author Sergio Bossa
 */
public interface BackupManager {

    /**
     * Backup export.<br>
     * Must not interrupt other bucket operations.
     *
     * @param bucket Bucket whose entries must be exported.
     * @param destination The destination resource where to write exported entries.
     * @throws StoreOperationException If errors occur.
     */
    public void exportBackup(Bucket bucket, String destination) throws StoreOperationException;

    /**
     * Backup import.<br>
     * Must not interrupt other bucket operations and must preserve already existent entries not contained
     * into the imported backup.
     *
     * @param bucket Bucket into which import entries.
     * @param source The source resource where to read entries from.
     * @throws StoreOperationException If errors occur.
     */
    public void importBackup(Bucket bucket, String source) throws StoreOperationException;
}
