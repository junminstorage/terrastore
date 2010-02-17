package terrastore.event.impl;

import terrastore.event.EventListener;

/**
 * @author Sergio Bossa
 */
public class ValueChangedEvent extends AbstractEvent {

    public ValueChangedEvent(String bucket, String key, byte[] value) {
        super(bucket, key, value);
    }

    @Override
    protected void doDispatch(EventListener listener) {
        listener.onValueChanged(key, value);
    }
}
