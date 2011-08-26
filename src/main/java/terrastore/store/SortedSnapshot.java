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

import org.fusesource.hawtdb.api.TxPageFileFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.fusesource.hawtbuf.codec.Codec;
import org.fusesource.hawtbuf.codec.StringCodec;
import org.fusesource.hawtdb.api.BTreeIndexFactory;
import org.fusesource.hawtdb.api.SortedIndex;
import org.fusesource.hawtdb.api.Transaction;
import static terrastore.startup.Constants.*;

/**
 * Sorted snapshot of keys.
 *
 * @author Sergio Bossa
 */
public class SortedSnapshot {

    private final ReadWriteLock stateLock;
    private final Comparator<String> comparator;
    private TxPageFileFactory pageFactory;
    private BTreeIndexFactory<String, String> indexFactory;
    private long timestamp;

    public SortedSnapshot(String name, Set<Key> keys, Comparator<String> comparator) {
        this.stateLock = new ReentrantReadWriteLock();
        this.comparator = comparator;
        computeIndex(getFile(name), keys, comparator);
    }

    public Set<Key> keysInRange(Key start, Key end, int limit) {
        stateLock.readLock().lock();
        try {
            return queryIndex(start, end, limit);
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public boolean isExpired(long timeToLive) {
        stateLock.readLock().lock();
        try {
            long now = System.currentTimeMillis();
            return (now - timestamp) >= timeToLive;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public void update(Set<Key> keys) {
        stateLock.writeLock().lock();
        try {
            recomputeIndex(keys);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    public void discard() {
        stateLock.writeLock().lock();
        try {
            Transaction tx = pageFactory.getTxPageFile().tx();
            SortedIndex<String, String> index = indexFactory.openOrCreate(tx);
            try {
                index.clear();
            } finally {
                tx.commit();
                tx.flush();
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    private File getFile(String fileName) {
        String homeDir = System.getenv(TERRASTORE_HOME) != null ? System.getenv(TERRASTORE_HOME) : System.getProperty(TERRASTORE_HOME);
        homeDir = homeDir != null ? homeDir : System.getProperty("java.io.tmpdir");
        if (homeDir != null) {
            String separator = System.getProperty("file.separator");
            File file = new File(homeDir + separator + SNAPSHOTS_DIR + separator + fileName + ".hdb");
            file.delete();
            return file;
        } else {
            throw new IllegalStateException("Terrastore home directory is not set!");
        }
    }

    private void computeIndex(File file, Set<Key> keys, Comparator<String> comparator) {
        pageFactory = new TxPageFileFactory();
        pageFactory.setPageSize((short) 512);
        pageFactory.setFile(file);
        pageFactory.open();

        indexFactory = new BTreeIndexFactory<String, String>();
        indexFactory.setComparator(comparator);
        indexFactory.setKeyCodec(StringCodec.INSTANCE);
        indexFactory.setValueCodec(new Codec<String>() {

            @Override
            public void encode(String object, DataOutput dataOut) throws IOException {
            }

            @Override
            public String decode(DataInput dataIn) throws IOException {
                return "";
            }

            @Override
            public int getFixedSize() {
                return 0;
            }

            @Override
            public int estimatedSize(String object) {
                return 0;
            }

            @Override
            public boolean isDeepCopySupported() {
                return true;
            }

            @Override
            public String deepCopy(String source) {
                return "";
            }

            @Override
            public boolean isEstimatedSizeSupported() {
                return true;
            }

        });

        Transaction tx = pageFactory.getTxPageFile().tx();
        SortedIndex<String, String> index = indexFactory.openOrCreate(tx);
        try {
            for (Key key : keys) {
                index.put(key.toString(), "");
            }
        } finally {
            tx.commit();
            tx.flush();
        }

        timestamp = System.currentTimeMillis();
    }

    private void recomputeIndex(final Set<Key> keys) {
        Transaction tx = pageFactory.getTxPageFile().tx();
        SortedIndex<String, String> index = indexFactory.openOrCreate(tx);
        try {
            index.clear();
            for (Key key : keys) {
                index.put(key.toString(), "");
            }
        } finally {
            tx.commit();
            tx.flush();
        }

        timestamp = System.currentTimeMillis();
    }

    private Set<Key> queryIndex(Key start, Key end, int limit) {
        String startString = start.toString();
        String endString = end == null ? null : end.toString();
        Transaction tx = pageFactory.getTxPageFile().tx();
        SortedIndex<String, String> index = indexFactory.openOrCreate(tx);
        try {
            LinkedHashSet<Key> result = new LinkedHashSet<Key>();
            Iterator<Entry<String, String>> entries = index.iterator(startString);
            int counter = 1;
            while (entries.hasNext()) {
                Entry<String, String> entry = entries.next();
                if ((endString == null || comparator.compare(entry.getKey(), endString) <= 0) && (limit == 0 || counter++ <= limit)) {
                    result.add(new Key(entry.getKey()));
                } else {
                    break;
                }
            }
            return result;
        } finally {
            tx.commit();
        }
    }

}
