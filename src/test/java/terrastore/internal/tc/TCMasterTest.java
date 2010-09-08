package terrastore.internal.tc;

import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class TCMasterTest {

    @Test
    public void testConnectionFailure() {
        TCMaster master = TCMaster.getInstance();
        assertFalse(master.connect("localhost:9510", 1, TimeUnit.SECONDS));
    }
}
