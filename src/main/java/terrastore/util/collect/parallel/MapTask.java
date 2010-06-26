package terrastore.util.collect.parallel;

import java.util.Collection;

/**
 * @author Sergio Bossa
 */
public interface MapTask<I, O, C extends Collection<O>> {

    public C map(I input, MapCollector<O, C> outputCollector);
}
