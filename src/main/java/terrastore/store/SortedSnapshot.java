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
package terrastore.store;

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
import org.fusesource.hawtdb.api.PageFileFactory;
import org.fusesource.hawtdb.api.Predicate;
import org.fusesource.hawtdb.api.SortedIndex;

/**
 * Sorted snapshot of keys.
 *
 * @author Sergio Bossa
 */
public class SortedSnapshot {

    private final ReadWriteLock stateLock;
    private final Comparator<String> comparator;
    private PageFileFactory pageFactory;
    private BTreeIndexFactory<String, String> indexFactory;
    private long timestamp;

    public SortedSnapshot(String name, Set<String> keys, Comparator<String> comparator) {
        this.stateLock = new ReentrantReadWriteLock();
        this.comparator = comparator;
        computeIndex(getFile(name), keys, comparator);
    }

    public Set<String> keysInRange(String start, String end, int limit) {
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

    public void update(Set<String> keys) {
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
            pageFactory.close();
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    private File getFile(String name) {
        try {
            return File.createTempFile("terrastore", name);
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to create snapshot file!");
        }
    }

    private void computeIndex(File file, Set<String> keys, Comparator<String> comparator) {
        pageFactory = new PageFileFactory();
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

        SortedIndex<String, String> index = indexFactory.create(pageFactory.getPageFile());
        for (String key : keys) {
            index.put(key, "");
        }
        pageFactory.getPageFile().flush();

        timestamp = System.currentTimeMillis();
    }

    private void recomputeIndex(final Set<String> keys) {
        SortedIndex<String, String> index = indexFactory.open(pageFactory.getPageFile());
        // Put new keys:
        for (String key : keys) {
            index.putIfAbsent(key, "");
        }
        // Remove old keys still in index:
        int surplus = index.size() - keys.size();
        if (surplus > 0) {
            Iterator<Entry<String, String>> iterator = index.iterator(new Predicate<String>() {

                @Override
                public boolean isInterestedInKeysBetween(String first, String second, Comparator comparator) {
                    return true;
                }

                @Override
                public boolean isInterestedInKey(String key, Comparator comparator) {
                    return !keys.contains(key);
                }
            });
            while (iterator.hasNext() && surplus-- > 0) {
                Entry<String, String> entry = iterator.next();
                index.remove(entry.getKey());
            }
        }
        //
        pageFactory.getPageFile().flush();

        timestamp = System.currentTimeMillis();
    }

    private Set<String> queryIndex(String start, String end, int limit) {
        SortedIndex<String, String> index = indexFactory.open(pageFactory.getPageFile());
        LinkedHashSet<String> result = new LinkedHashSet<String>();
        Iterator<Entry<String, String>> entries = index.iterator(start);
        int counter = 1;
        while (entries.hasNext()) {
            Entry<String, String> entry = entries.next();
            if ((end == null || comparator.compare(entry.getKey(), end) <= 0) && (limit == 0 || counter++ <= limit)) {
                result.add(entry.getKey());
            } else {
                break;
            }
        }
        return result;
    }
}
