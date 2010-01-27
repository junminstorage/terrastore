package terrastore.communication.seda;

import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.Command;

/**
 * @author Sergio Bossa
 */
public interface SEDAProcessor {

    public void start();

    public void pause();

    public void resume();

    public void stop();

    public <R> R process(Command<R> command) throws ProcessingException;
}
