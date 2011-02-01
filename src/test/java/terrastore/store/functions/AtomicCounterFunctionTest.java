/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.store.functions;

import terrastore.store.functions.support.FunctionTestFixture;
import static terrastore.store.functions.support.MapUtil.*;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Sven Johansson
 */
public class AtomicCounterFunctionTest extends FunctionTestFixture {

    @Before
    public void setUp() {
        function = new AtomicCounterFunction();
    }
    
    @Test
    public void singleFieldWithMatchingPlusOneCounterIncrementsByOne() {
        givenOriginalValue(
            map(entry("count", "1"))
        );
        
        whenFunctionIsAppliedWithParameters(
            map(entry("count", "1"))
        );
        
        thenResultEquals(
            map(entry("count", "2"))
        );
    }

    @Test
    public void supportsNumberCounters() {
        givenOriginalValue(
            map(entry("count", 1L))
        );

        whenFunctionIsAppliedWithParameters(
            map(entry("count", 1L))
        );

        thenResultEquals(
            map(entry("count", 2L))
        );
    }

    @Test
    public void otherFieldsAreUnaffectedByCounterOperation() {
        givenOriginalValue(
            map(
                entry("count", "1"),
                entry("field", "value")
            )
        );
      
        whenFunctionIsAppliedWithParameters(
            map(entry("count", "1"))
        );

        thenResultEquals(
            map(
                entry("count", "2"),
                entry("field", "value")
            )
        );
    }
    
    @Test
    public void nonIntegerParameterThrowsException() {
        givenOriginalValue(
            map(entry("count", "1"))
        );
            
        whenFunctionIsAppliedWithParameters(
            map(entry("count", "NaN"))
        );
        
        thenExceptionHasBeenThrown(IllegalArgumentException.class);
    }
 
    @Test
    public void fieldIsCreatedWhenItDoesNotExist() {
        givenOriginalValue(map());

        whenFunctionIsAppliedWithParameters(
            map(entry("count", "1"))
        );
        
        thenResultEquals(
            map(entry("count", "1"))
        );
    }
    
    @Test
    public void nonIntegerOriginalValueThrowsException() {
        givenOriginalValue(
            map(entry("count", "NaN"))
        );
        
        whenFunctionIsAppliedWithParameters(
            map(entry("count", "1"))
        );
        
        thenExceptionHasBeenThrown(IllegalArgumentException.class);
    }
    
    @Test
    public void embeddedFieldIsCreatedWhenItDoesNotExist() {
        givenOriginalValue(
            map(
                entry("embedded", map(
                    entry("count", "1"))
                )
            )
        );
        
        whenFunctionIsAppliedWithParameters(
            map(
                entry("embedded", map(
                    entry("count", "1"))
                )
            )
        );
        
        thenResultEquals(
            map(
                entry("embedded", map(
                    entry("count", "2"))
                )
            )
        );
    }
    
    @Test
    public void attemptToApplyCounterOnEmbeddedMapThrowsException() {
        givenOriginalValue(
            map(
                entry("embedded", map(
                    entry("count", "1"))
                )
            )
        );
        
        whenFunctionIsAppliedWithParameters(
            map(
                entry("embedded", "1")
            )
        );
        
        thenExceptionHasBeenThrown(IllegalArgumentException.class);
    }
    
    @Test
    public void referenceToEmbeddedObjectBlockedByAValueThrowsException() {
        givenOriginalValue(
            map(
                entry("embedded", "1")
            )
        );
        
        whenFunctionIsAppliedWithParameters(
            map(
                entry("embedded", map(
                    entry("count", "1"))
                )
            )
        );
        
        thenExceptionHasBeenThrown(IllegalArgumentException.class);
    }

    @Test
    public void embeddedFieldThatDoesNotExistIsCreatedByPlusOneCounter() {
        givenOriginalValue(map());
        
        whenFunctionIsAppliedWithParameters(
            map(
                entry("embedded", map(
                    entry("count", "1"))
                )
            )
        );
        
        thenResultEquals(
            map(
                entry("embedded", map(
                    entry("count", "1"))
                )
            )
        );
    }

}
