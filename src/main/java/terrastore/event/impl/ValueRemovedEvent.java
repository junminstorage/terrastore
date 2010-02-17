package terrastore.event.impl;

import terrastore.event.EventListener;

/**
 * @author Sergio Bossa
 */
public class ValueRemovedEvent extends AbstractEvent {

    public ValueRemovedEvent(String bucket, String key) {
        super(bucket, key, null);
    }

    @Override
    protected void doDispatch(EventListener listener) {
        listener.onValueRemoved(getKey());
    }
}
