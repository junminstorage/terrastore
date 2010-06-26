package terrastore.util.collect.parallel;

import java.util.Collection;

/**
 * @author Sergio Bossa
 */
public interface MapCollector<O, C extends Collection<O>> {

    public C createCollector();
}
