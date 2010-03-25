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

import org.easymock.classextension.EasyMock;
import org.junit.Test;
import terrastore.communication.Node;
import terrastore.communication.protocol.ExportBackupCommand;
import terrastore.communication.protocol.ImportBackupCommand;
import terrastore.router.Router;
import terrastore.service.BackupOperationException;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class DefaultBackupServiceTest {

    @Test
    public void testImportBackup() throws Exception {
        Node node = createMock(Node.class);
        Router router = createMock(Router.class);

        router.routeToLocalNode();
        expectLastCall().andReturn(node).once();
        node.send(EasyMock.<ImportBackupCommand>anyObject());
        expectLastCall().andReturn(null).once();

        replay(node, router);

        DefaultBackupService service = new DefaultBackupService(router, "secret");
        service.importBackup("bucket", "source", "secret");

        verify(node, router);
    }

    @Test(expected = BackupOperationException.class)
    public void testImportBackupWithBadSecret() throws Exception {
        Router router = createMock(Router.class);

        replay(router);

        DefaultBackupService service = new DefaultBackupService(router, "secret");
        try {
            service.importBackup("bucket", "source", "bad");
        } finally {
            verify(router);
        }
    }

    @Test
    public void testExportBackup() throws Exception {
        Node node = createMock(Node.class);
        Router router = createMock(Router.class);

        router.routeToLocalNode();
        expectLastCall().andReturn(node).once();
        node.send(EasyMock.<ExportBackupCommand>anyObject());
        expectLastCall().andReturn(null).once();

        replay(node, router);

        DefaultBackupService service = new DefaultBackupService(router, "secret");
        service.exportBackup("bucket", "destination", "secret");

        verify(node, router);
    }

    @Test(expected = BackupOperationException.class)
    public void testExportBackupWithBadSecret() throws Exception {
        Router router = createMock(Router.class);

        replay(router);

        DefaultBackupService service = new DefaultBackupService(router, "secret");
        try {
            service.exportBackup("bucket", "destination", "bad");
        } finally {
            verify(router);
        }
    }
}
