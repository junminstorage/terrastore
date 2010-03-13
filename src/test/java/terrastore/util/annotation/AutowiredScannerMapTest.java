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
package terrastore.util.annotation;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import terrastore.annotation.Autowired;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class AutowiredScannerMapTest {

    @Test
    public void testPresetsOverwriteAutowiredInCaseOfConflictingName() {
        Map presetsMap = new HashMap();
        presetsMap.put("one", new PresetOne());
        //
        AutowiredScannerMap scannerMap = new AutowiredScannerMap(presetsMap, new AutowiredScanner(), AutowiredCommon.class);
        //
        assertEquals(2, scannerMap.size());
        assertEquals(PresetOne.class, scannerMap.get("one").getClass());
        assertEquals(AutowiredTwo.class, scannerMap.get("two").getClass());
    }

    public static class PresetOne {
    }

    public static interface AutowiredCommon {
    }

    @Autowired(name = "one", order = 1)
    public static class AutowiredOne implements AutowiredCommon {
    }

    @Autowired(name = "two", order = 2)
    public static class AutowiredTwo implements AutowiredCommon {
    }
}
