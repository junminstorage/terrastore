package terrastore.service.conditions;

import java.util.HashMap;
import org.junit.Test;
import terrastore.util.collect.Maps;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class IfConditionTest {

    @Test
    public void testIfAbsentCondition() {
        IfCondition condition = new IfCondition();
        assertFalse(condition.isSatisfied("key", new HashMap<String, Object>(), "absent()"));
    }

    @Test
    public void testSuccessfulIfMatchesCondition() {
        IfCondition condition = new IfCondition();
        assertTrue(condition.isSatisfied("key", Maps.hash(new String[]{"key"}, new Object[]{"test"}), "matches(key,test)"));
    }

    @Test
    public void testUnsuccessfulIfMatchesCondition() {
        IfCondition condition = new IfCondition();
        assertFalse(condition.isSatisfied("key", Maps.hash(new String[]{"key"}, new Object[]{"unmatched"}), "matches(key,test)"));
    }
}