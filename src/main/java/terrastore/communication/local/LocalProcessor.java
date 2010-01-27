package terrastore.communication.local;

import terrastore.communication.seda.AbstractSEDAProcessor;
import terrastore.store.Store;

/**
 * @author Sergio Bossa
 */
public class LocalProcessor extends AbstractSEDAProcessor {

    public LocalProcessor(Store store, int threads) {
        super(store, threads);
    }
}
