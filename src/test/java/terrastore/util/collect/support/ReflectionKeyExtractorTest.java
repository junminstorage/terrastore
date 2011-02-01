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
package terrastore.util.collect.support;

import terrastore.util.collect.support.KeyExtractor;
import terrastore.util.collect.support.ReflectionKeyExtractor;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class ReflectionKeyExtractorTest {

    @Test
    public void testKeyExtraction() {
        KeyExtractor<String, TestBean> extractor = new ReflectionKeyExtractor<String, TestBean>("property");
        assertEquals("value", extractor.extractFrom(new TestBean("value")));
    }

    @Test(expected=IllegalStateException.class)
    public void testKeyExtractionOnWrongField() {
        KeyExtractor<String, TestBean> extractor = new ReflectionKeyExtractor<String, TestBean>("wrong");
        extractor.extractFrom(new TestBean("value"));
    }

    private static class TestBean {

        private final String property;

        public TestBean(String property) {
            this.property = property;
        }
    }
}