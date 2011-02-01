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
