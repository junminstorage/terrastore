/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.communication.remote.pipe;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.terracotta.modules.annotations.InstrumentedClass;
import terrastore.communication.serialization.Serializer;

/**
 * Remote communication channel connecting different cluster nodes.
 *
 * @author Sergio Bossa
 */
@InstrumentedClass
public class Pipe<T> {

    /**
     * TODO
     * Add (and use) a "batching" take method to speed up retrieving of messages.
     * Implement by using an array with inner-pointers to try avoiding lock contention?
     */

    private final BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
    private final Serializer<T> serializer;

    public Pipe(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    public T put(T message) throws InterruptedException {
        String serialized = serializer.serialize(message);
        queue.put(serialized);
        return message;
    }

    public T poll() {
        String serialized = queue.poll();
        if (serialized != null) {
            return serializer.deserialize(serialized);
        } else {
            return null;
        }
    }

    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        String serialized = queue.poll(timeout, unit);
        if (serialized != null) {
            return serializer.deserialize(serialized);
        } else {
            return null;
        }
    }

    public T take() throws InterruptedException {
        String serialized = queue.take();
        return serializer.deserialize(serialized);
    }

    public T peek() {
        String serialized = queue.peek();
        if (serialized != null) {
            return serializer.deserialize(serialized);
        } else {
            return null;
        }
    }

    public boolean offer(T message) {
        String serialized = serializer.serialize(message);
        return queue.offer(serialized);
    }

    public boolean offer(T message, final long timeout, final TimeUnit unit) throws InterruptedException {
        String serialized = serializer.serialize(message);
        return queue.offer(serialized, timeout, unit);
    }

    public void clear() {
        queue.clear();
    }

    public int size() {
        return queue.size();
    }
}
