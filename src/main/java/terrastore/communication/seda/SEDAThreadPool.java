package terrastore.communication.seda;

import java.util.concurrent.Future;

/**
 * @author Sergio Bossa
 */
public interface SEDAThreadPool {

    public <R> Future<R> execute(ExecutionHandler<R> handler);

    public void pause();

    public void resume();

    public void shutdown();

    public boolean isPaused();
}
