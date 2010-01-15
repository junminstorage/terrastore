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

import java.io.UnsupportedEncodingException;
import java.util.Map;
import org.junit.Test;
import terrastore.store.types.JsonValue;
import terrastore.util.JsonUtils;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class JXPathConditionTest {

    private static final String JSON_VALUE = "{\"key\" : \"value\", \"array\" : [\"primitive\"]}";

    @Test
    public void testSatisfiedWithJsonValue() throws UnsupportedEncodingException {
        Map<String, Object> json = JsonUtils.toMap(new JsonValue(JSON_VALUE.getBytes("UTF-8")));
        String jxpath = "/key[.='value']";
        JXPathCondition condition = new JXPathCondition();
        assertTrue(condition.isSatisfied("ignored", json, jxpath));
    }

    @Test
    public void testNotSatisfiedWithJsonValue() throws UnsupportedEncodingException {
        Map<String, Object> json = JsonUtils.toMap(new JsonValue(JSON_VALUE.getBytes("UTF-8")));
        String jxpath = "/key[.='wrong']";
        JXPathCondition condition = new JXPathCondition();
        assertFalse(condition.isSatisfied("ignored", json, jxpath));
    }
}