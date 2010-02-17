package terrastore.event;

/**
 * @author Sergio Bossa
 */
public interface EventListener {

    public boolean observes(String bucket);

    public void onValueChanged(String key, byte[] value);

    public void onValueRemoved(String key);
}
