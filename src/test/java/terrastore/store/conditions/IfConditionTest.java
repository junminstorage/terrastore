/**
 * Copyright 2009 - 2010 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.store.conditions;

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