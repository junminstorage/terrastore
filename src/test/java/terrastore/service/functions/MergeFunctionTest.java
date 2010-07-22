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

import terrastore.service.functions.support.FunctionTestFixture;
import static terrastore.service.functions.support.MapUtil.*;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Sven Johansson
 */
public class MergeFunctionTest extends FunctionTestFixture {

    @Before
    public void setUp() {
        function = new MergeFunction();
    }
    
    @Test
    public void mapsAreMergedWhenFunctionIsApplied() {
        givenOriginalValue(
            map(
                entry("value1", "a value"),
                entry("value2", "another value")
            )
        );
        
        whenFunctionIsAppliedWithParameters(
            map(
                entry("value2", "another replaced value"),
                entry("value3", "a third value")
            )
        );
        
        thenResultEquals(
           map(
                entry("value1", "a value"),
                entry("value2", "another replaced value"),
                entry("value3", "a third value")
           )
        );
    }
    
    @Test
    public void unreferencedSiblingsInEmbeddedObjectsAreRetained() {
        givenOriginalValue(
            map(
                entry("embedded", map(
                    entry("value1", "a value")       
                ))
            )
        );
        
        whenFunctionIsAppliedWithParameters(
            map(
                entry("embedded", map(
                    entry("value2", "another value")      
                ))
            )
        );
        
        thenResultEquals(
            map(
                entry("embedded", map(
                    entry("value1", "a value"),
                    entry("value2", "another value")
                ))
            )
        );
    }
    
    @Test
    public void valueCanBeReplacedByEmbeddedObject() {
        givenOriginalValue(
            map(
                entry("value1", "a value"),
                entry("value2", "another value")
            )
        );
        
        whenFunctionIsAppliedWithParameters(
            map(
                entry("value2", map(
                   entry("value", "an embedded value")     
                ))
            )
        );
        
        thenResultEquals(
            map(
                entry("value1", "a value"),
                entry("value2", map(
                    entry("value", "an embedded value")     
                ))
            )
        );
    }

    @Test
    public void embeddedObjectIsAdded() {
        givenOriginalValue(
            map(
                entry("value1", "a value")
            )
        );

        whenFunctionIsAppliedWithParameters(
            map(
                entry("value2", map(
                   entry("value", "an embedded value")
                ))
            )
        );

        thenResultEquals(
            map(
                entry("value1", "a value"),
                entry("value2", map(
                   entry("value", "an embedded value")
                ))
            )
        );
    }
    
    @Test
    public void embeddedObjectCanBeReplacedByValue() {
        givenOriginalValue(
            map(
                entry("value1", "a value"),
                entry("value2", map(
                   entry("value", "an embedded value")     
                ))
            )
        );
        
        whenFunctionIsAppliedWithParameters(
            map(
                entry("value2", "another value")
            )
        );
        
        thenResultEquals(
            map(
                entry("value1", "a value"),
                entry("value2", "another value")
            )
        );
    }   
}
