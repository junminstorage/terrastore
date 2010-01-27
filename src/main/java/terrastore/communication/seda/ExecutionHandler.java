package terrastore.communication.seda;

import java.util.concurrent.Callable;
import terrastore.communication.protocol.Command;
import terrastore.store.Store;

/**
 * @author Sergio Bossa
 */
public class ExecutionHandler<R> implements Callable<R> {

    private final Store store;
    private final Command<R> command;

    public ExecutionHandler(Store store, Command<R> command) {
        this.store = store;
        this.command = command;
    }

    @Override
    public R call() throws Exception {
        return command.executeOn(store);
    }
}
