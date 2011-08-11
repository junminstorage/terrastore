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
package terrastore.backup;

import java.io.File;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import terrastore.communication.Node;
import terrastore.communication.protocol.GetKeysCommand;
import terrastore.router.Router;
import terrastore.startup.Constants;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.util.collect.Sets;
import org.easymock.classextension.EasyMock;
import terrastore.backup.impl.DefaultBackupExporter;
import terrastore.backup.impl.DefaultBackupImporter;
import terrastore.communication.protocol.GetValueCommand;
import terrastore.communication.protocol.PutValueCommand;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class BackupTest {

    @Before
    public void setUp() {
        System.setProperty(Constants.TERRASTORE_HOME, System.getProperty("java.io.tmpdir"));
        new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + Constants.BACKUPS_DIR).mkdir();
    }

    @Test
    public void testExportImport() throws Exception {
        String bucket = "bucket";
        Set<Key> keys = Sets.linked(new Key("1"), new Key("2"), new Key("3"));
        Value v1 = new Value("v1".getBytes());
        Value v2 = new Value("v2".getBytes());
        Value v3 = new Value("v3".getBytes());

        Node node = createMock(Node.class);
        node.send(EasyMock.<GetKeysCommand>anyObject());
        expectLastCall().andReturn(keys).times(1);
        node.send(EasyMock.<GetValueCommand>anyObject());
        expectLastCall().andReturn(v1).times(1).andReturn(v2).times(1).andReturn(v3).times(1);
        node.send(EasyMock.<PutValueCommand>anyObject());
        expectLastCall().andReturn(null).times(3);

        Router router = createMock(Router.class);
        router.routeToLocalNode();
        expectLastCall().andReturn(node).times(1);
        router.routeToNodeFor(bucket, new Key("1"));
        expectLastCall().andReturn(node).times(2);
        router.routeToNodeFor(bucket, new Key("2"));
        expectLastCall().andReturn(node).times(2);
        router.routeToNodeFor(bucket, new Key("3"));
        expectLastCall().andReturn(node).times(2);

        replay(node, router);

        BackupExporter exporter = new DefaultBackupExporter();
        BackupImporter importer = new DefaultBackupImporter();
        exporter.exportBackup(router, bucket, "b.bak");
        importer.importBackup(router, bucket, "b.bak");

        verify(node, router);
    }

}
