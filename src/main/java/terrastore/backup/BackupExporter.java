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

import terrastore.store.Bucket;
import terrastore.store.StoreOperationException;

/**
 * Execute export of {@link Bucket} entries.<br>
 *
 * @author Sergio Bossa
 */
public interface BackupExporter {

    /**
     * Backup export.<br>
     * Must not interrupt other bucket operations.
     *
     * @param bucket Bucket whose entries must be exported.
     * @param destination The destination resource where to write exported entries.
     * @throws BackupOperationException If errors occur.
     */
    public void exportBackup(Bucket bucket, String destination) throws BackupException;
}
