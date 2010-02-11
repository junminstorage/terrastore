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

        router.getLocalNode();
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

        router.getLocalNode();
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
