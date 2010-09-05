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
package terrastore.util.collect.support;

import java.lang.reflect.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Sergio Bossa
 */
public class ReflectionKeyExtractor<K, V> implements KeyExtractor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(ReflectionKeyExtractor.class);
    private final String fieldName;

    public ReflectionKeyExtractor(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public K extractFrom(V value) {
        try {
            Field field = value.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (K) field.get(value);
        } catch (Exception ex) {
            LOG.warn(ex.getMessage(), ex);
            throw new IllegalStateException(ex);
        }
    }
}
