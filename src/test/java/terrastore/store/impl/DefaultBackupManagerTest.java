package terrastore.store.impl;

import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.Charset;
import org.junit.Before;
import org.junit.Test;
import terrastore.startup.Constants;
import terrastore.store.BackupManager;
import terrastore.store.Bucket;
import terrastore.store.types.JsonValue;
import static org.easymock.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class DefaultBackupManagerTest {

    private static final String KEY_1 = "KEY1";
    private static final String KEY_2 = "KEY2";
    private static final JsonValue JSON_VALUE_1 = new JsonValue("{\"test1\":\"test1\"}".getBytes(Charset.forName("UTF-8")));
    private static final JsonValue JSON_VALUE_2 = new JsonValue("{\"test2\":\"test2\"}".getBytes(Charset.forName("UTF-8")));

    @Before
    public void setUp() {
        System.setProperty(Constants.TERRASTORE_HOME, System.getProperty("java.io.tmpdir"));
        new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + Constants.BACKUPS_DIR).mkdir();
    }

    @Test
    public void testExportImportWithJsonValue() throws Exception {
        Bucket bucket = createMock(Bucket.class);

        bucket.keys();
        expectLastCall().andReturn(Sets.newHashSet(KEY_1, KEY_2));
        bucket.get(KEY_1);
        expectLastCall().andReturn(JSON_VALUE_1).once();
        bucket.get(KEY_2);
        expectLastCall().andReturn(JSON_VALUE_2).once();
        bucket.put(KEY_1, JSON_VALUE_1);
        expectLastCall().once();
        bucket.put(KEY_2, JSON_VALUE_2);
        expectLastCall().once();

        replay(bucket);

        BackupManager backupManager = new DefaultBackupManager();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        backupManager.exportBackup(bucket, "test");

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        backupManager.importBackup(bucket, "test");

        verify(bucket);
    }

}