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