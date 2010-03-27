package terrastore.communication;

import terrastore.communication.protocol.Command;

/**
 * @author Sergio Bossa
 */
public interface Processor {

    public <R> R process(Command<R> command) throws ProcessingException;
}
