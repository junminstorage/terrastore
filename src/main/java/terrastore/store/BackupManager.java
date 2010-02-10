package terrastore.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Sergio Bossa
 */
public interface BackupManager {

    public void exportBackup(Bucket bucket, String destination) throws StoreOperationException;

    public void importBackup(Bucket bucket, String source) throws StoreOperationException;
}
