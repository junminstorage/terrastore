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

package terrastore.service.functions;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sven Johansson
 */
public class MergeFunctionTest {

    @Test
    public void testMapsAreMergedWhenFunctionIsApplied() {
        Map<String, Object> originalValue = new HashMap<String, Object>();
        originalValue.put("value1", "a value");
        originalValue.put("value2", "another value");
        
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("value2", "another replaced value");
        parameters.put("value3", "a third value");
        
        Map<String, Object> resultingValue = new MergeFunction().apply("KEY1", originalValue, parameters);
        
        assertEquals("a value", resultingValue.get("value1"));
        assertEquals("another replaced value", resultingValue.get("value2"));
        assertEquals("a third value", resultingValue.get("value3"));
    }
    
}
