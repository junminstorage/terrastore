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
package terrastore.service.conditions;

import org.junit.Test;
import terrastore.store.types.JsonValue;
import terrastore.util.io.InputReader;
import terrastore.util.json.JsonUtils;

import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Giuseppe Santoro
 */
public class JSConditionTest {

    @Test
    public void testSatisfiedWithJsonValue() throws Exception {
        String input = new String(new InputReader().read(this.getClass().getClassLoader().getResourceAsStream("jxpath.json")));
        Map<String, Object> json = JsonUtils.toUnmodifiableMap(new JsonValue(input.getBytes("UTF-8")));
        String function = "value['favorited']";
        JSCondition condition = new JSCondition();
        assertFalse(condition.isSatisfied("key", json, function));
    }

    @Test
    public void testNotSatisfiedWithJsonValue() throws Exception {
        String input = new String(new InputReader().read(this.getClass().getClassLoader().getResourceAsStream("jxpath.json")));
        Map<String, Object> json = JsonUtils.toUnmodifiableMap(new JsonValue(input.getBytes("UTF-8")));
        String function = "value.id == 6626190681";
        JSCondition condition = new JSCondition();
        assertTrue(condition.isSatisfied("key", json, function));
    }
}