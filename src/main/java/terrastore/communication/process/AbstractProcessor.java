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
package terrastore.communication.process;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.Command;
import terrastore.store.StoreOperationException;

/**
 * @author Sergio Bossa
 */
public abstract class AbstractProcessor implements Processor {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractProcessor.class);
    //
    private final ThreadPool threadPool;

    public AbstractProcessor(int threads) {
        this.threadPool = new ExecutorBasedThreadPool(threads);
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
    public final <R> R process(final Command<R> command, final CommandHandler<R> commandHandler) throws ProcessingException {
        try {
            Future<R> future = threadPool.<R>execute(new SyncCallable<R>(command, commandHandler));
            return future.get();
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof StoreOperationException) {
                throw new ProcessingException(((StoreOperationException) ex.getCause()).getErrorMessage());
            } else if (ex.getCause() instanceof ProcessingException) {
                throw ((ProcessingException) ex.getCause());
            } else {
                throw new ProcessingException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getCause().getMessage()));
            }
        } catch (Exception ex) {
            throw new ProcessingException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
        }
    }

    @Override
    public <R> void process(Command<R> command, CommandHandler<R> commandHandler, CompletionHandler<R, ProcessingException> completionHandler) {
        threadPool.<R>execute(new AsyncCallable<R>(command, commandHandler, completionHandler));
    }

    protected void doStart() {
    }

    protected void doStop() {
    }

    private static class SyncCallable<R> implements Callable<R> {

        private final Command<R> command;
        private final CommandHandler<R> commandHandler;

        public SyncCallable(Command<R> command, CommandHandler<R> commandHandler) {
            this.command = command;
            this.commandHandler = commandHandler;
        }

        @Override
        public R call() throws Exception {
            return commandHandler.handle(command);
        }
    }

    private static class AsyncCallable<R> implements Callable<R> {

        private final Command<R> command;
        private final CommandHandler<R> commandHandler;
        private final CompletionHandler<R, ProcessingException> completionHandler;

        public AsyncCallable(Command<R> command, CommandHandler<R> commandHandler, CompletionHandler<R, ProcessingException> completionHandler) {
            this.command = command;
            this.commandHandler = commandHandler;
            this.completionHandler = completionHandler;
        }

        @Override
        public R call() throws Exception {
            try {
                R result = commandHandler.handle(command);
                completionHandler.handleSuccess(result);
            } catch (Exception ex) {
                if (ex instanceof StoreOperationException) {
                    completionHandler.handleFailure(new ProcessingException(((StoreOperationException) ex).getErrorMessage()));
                } else if (ex instanceof ProcessingException) {
                    completionHandler.handleFailure((ProcessingException) ex);
                } else {
                    completionHandler.handleFailure(new ProcessingException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage())));
                }
            }
            return null;
        }
    }
}
