package terrastore.util;

import com.google.common.collect.AbstractIterator;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class LimitedSet<E> extends AbstractSet<E> {

    private final Set<E> sourceSet;
    private final int limit;

    public LimitedSet(Set<E> sourceSet, int limit) {
        this.sourceSet = sourceSet;
        this.limit = limit;
    }

    @Override
    public Iterator<E> iterator() {
        return new LimitedIterator(sourceSet.iterator());
    }

    @Override
    public int size() {
        if (limit > 0) {
            return sourceSet.size() < limit ? sourceSet.size() : limit;
        } else {
            return sourceSet.size();
        }
    }

    private class LimitedIterator extends AbstractIterator<E> {

        private int index = 0;
        private Iterator<E> sourceIterator;

        public LimitedIterator(Iterator<E> sourceIterator) {
            this.sourceIterator = sourceIterator;
        }

        @Override
        protected E computeNext() {
            if ((limit == 0 || index < limit) && sourceIterator.hasNext()) {
                index++;
                return sourceIterator.next();
            } else {
                return endOfData();
            }
        }
    }
}
