package terrastore.communication.process;

/**
 * @author Sergio Bossa
 */
public interface CompletionHandler<R, E extends Exception> {

    public void handleSuccess(R response) throws Exception;

    public void handleFailure(E exception) throws Exception;
}
