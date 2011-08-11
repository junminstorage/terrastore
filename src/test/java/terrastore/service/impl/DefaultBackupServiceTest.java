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

import org.junit.Test;
import terrastore.backup.BackupExporter;
import terrastore.backup.BackupImporter;
import terrastore.router.Router;
import terrastore.service.BackupOperationException;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class DefaultBackupServiceTest {

    @Test
    public void testImportBackup() throws Exception {
        Router router = createMock(Router.class);
        BackupImporter backupImporter = createMock(BackupImporter.class);
        BackupExporter backupExporter = createMock(BackupExporter.class);

        backupImporter.importBackup(router, "bucket", "source");
        expectLastCall().once();

        replay(router, backupImporter, backupExporter);

        DefaultBackupService service = new DefaultBackupService(backupImporter, backupExporter, router, "secret");
        service.importBackup("bucket", "source", "secret");

        verify(router, backupImporter, backupExporter);
    }

    @Test(expected = BackupOperationException.class)
    public void testImportBackupWithBadSecret() throws Exception {
        Router router = createMock(Router.class);
        BackupImporter backupImporter = createMock(BackupImporter.class);
        BackupExporter backupExporter = createMock(BackupExporter.class);

        replay(router, backupImporter, backupExporter);

        DefaultBackupService service = new DefaultBackupService(backupImporter, backupExporter, router, "secret");
        try {
            service.importBackup("bucket", "source", "bad");
        } finally {
            verify(router, backupImporter, backupExporter);
        }
    }

    @Test
    public void testExportBackup() throws Exception {
        Router router = createMock(Router.class);
        BackupImporter backupImporter = createMock(BackupImporter.class);
        BackupExporter backupExporter = createMock(BackupExporter.class);

        backupExporter.exportBackup(router, "bucket", "destination");
        expectLastCall().once();

        replay(router, backupImporter, backupExporter);

        DefaultBackupService service = new DefaultBackupService(backupImporter, backupExporter, router, "secret");
        service.exportBackup("bucket", "destination", "secret");

        verify(router, backupImporter, backupExporter);
    }

    @Test(expected = BackupOperationException.class)
    public void testExportBackupWithBadSecret() throws Exception {
        Router router = createMock(Router.class);
        BackupImporter backupImporter = createMock(BackupImporter.class);
        BackupExporter backupExporter = createMock(BackupExporter.class);

        replay(router, backupImporter, backupExporter);

        DefaultBackupService service = new DefaultBackupService(backupImporter, backupExporter, router, "secret");
        try {
            service.exportBackup("bucket", "source", "bad");
        } finally {
            verify(router, backupImporter, backupExporter);
        }
    }

}
