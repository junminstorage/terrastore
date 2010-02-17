package terrastore.event;

/**
 * @author Sergio Bossa
 */
public interface Event {

    public String getBucket();

    public String getKey();

    public byte[] getValue();

    public void addEventListener(EventListener listener);

    public void dispatch();
}
