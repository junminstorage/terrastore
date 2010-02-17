package terrastore.event;

import java.util.List;

/**
 * @author Sergio Bossa
 */
public interface EventBus {

    public List<EventListener> getEventListeners();

    public void publish(Event event);

    public void shutdown();
}
