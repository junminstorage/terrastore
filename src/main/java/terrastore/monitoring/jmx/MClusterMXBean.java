package terrastore.monitoring.jmx;

/**
 * @author Sergio Bossa
 */
public interface MClusterMXBean {

    String getName();

    String getStatus();

    public enum Status {

        AVAILABLE,
        UNAVAILABLE;
    }
}
