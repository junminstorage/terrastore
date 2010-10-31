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

package terrastore.store.functions.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Map;

import terrastore.store.operators.Function;

/**
 * @author Sven Johansson
 */
public class FunctionTestFixture {

    protected Map<String, Object> originalValue;
    protected Map<String, Object> result;
    protected Exception thrownException;
    protected Function function;

    protected void givenOriginalValue(Map<String, Object> map) {
        this.originalValue = map;
    }

    protected void whenFunctionIsAppliedWithParameters(Map<String, Object> functionParameters) {
        try {
            this.result = function.apply("key", originalValue, functionParameters);
        } catch (Exception e) {
            thrownException = e;
        }
    }

    protected void thenResultEquals(Map<String, Object> expected) {
        assertNull("Unexpected exception: " + thrownException, thrownException);
        assertEquals(expected, result);
    }

    protected void thenExceptionHasBeenThrown(Class<? extends Exception> expectedExceptionType) {
        assertNotNull("An exception was expected.", thrownException);
        assertEquals("Not expected exception type.", expectedExceptionType, thrownException.getClass());
    }
    
    
    
}
