package terrastore.communication.seda;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import terrastore.common.ErrorMessage;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.Command;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;

/**
 * @author Sergio Bossa
 */
public abstract class AbstractSEDAProcessor implements SEDAProcessor {

    private final Store store;
    private final SEDAThreadPool threadPool;

    public AbstractSEDAProcessor(Store store, int threads) {
        this.store = store;
        this.threadPool = new SEDAThreadPoolExecutor(threads);
    }

    @Override
    public final void start() {
        doStart();
    }

    @Override
    public final void pause() {
        threadPool.pause();
    }

    @Override
    public final void resume() {
        threadPool.resume();
    }

    @Override
    public final void stop() {
        threadPool.shutdown();
        doStop();
    }

    @Override
    public final <R> R process(final Command<R> command) throws ProcessingException {
        try {
            Future<R> future = threadPool.<R>execute(new ExecutionHandler<R>(store, command));
            return future.get();
        } catch (ExecutionException ex) {
            throw new ProcessingException(((StoreOperationException) ex.getCause()).getErrorMessage());
        } catch (Exception ex) {
            throw new ProcessingException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
        }
    }

    protected void doStart() {
    }

    protected void doStop() {
    }
}
