package terrastore.util.collect;

/**
 * @author Sergio Bossa
 */
public interface Transformer<I, O> {

    public O transform(I input);
}
