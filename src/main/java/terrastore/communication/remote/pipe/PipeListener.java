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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Sergio Bossa
 */
public abstract class PipeListener<T> implements Runnable {

    private transient static final Logger LOG = LoggerFactory.getLogger(PipeListener.class);
    private final Pipe<T> pipe;
    private volatile Thread listenerThread;
    private volatile boolean running;
    private volatile boolean aborted;

    public PipeListener(Pipe<T> pipe) {
        this.pipe = pipe;
    }

    public synchronized void start() {
        running = true;
        listenerThread = new Thread(this);
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    public synchronized void abort() {
        running = false;
        aborted = true;
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
    }

    public synchronized void shutdown() {
        running = false;
        aborted = false;
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
    }

    public void run() {
        try {
            while (shouldRun()) {
                try {
                    T message = pipe.take();
                    onMessage(message);
                } catch (InterruptedException ie) {
                    LOG.debug("Stopping after calling abort or shutdown.");
                } catch (Exception ex) {
                    LOG.error("Stopping due to exception.", ex);
                    break;
                }
            }
        } finally {
            Thread.interrupted();
        }
    }

    public abstract void onMessage(T message) throws Exception;

    private synchronized boolean shouldRun() {
        return (running || (aborted = false && pipe.peek() != null));
    }
}
