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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.scannotation.AnnotationDB;
import org.scannotation.ClasspathUrlFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.annotation.AutoDetect;

/**
 * Scan for classes annotated with {@link terrastore.annotation.AutoDetect}.
 *
 * @author Sergio Bossa
 */
public class AnnotationScanner {

    private static final Logger LOG = LoggerFactory.getLogger(AnnotationScanner.class);
    //
    private final AnnotationDB annotations;

    public AnnotationScanner() {
        annotations = new AnnotationDB();
        annotations.setScanClassAnnotations(true);
        annotations.setScanFieldAnnotations(false);
        annotations.setScanMethodAnnotations(false);
        annotations.setScanParameterAnnotations(false);
        try {
            annotations.scanArchives(ClasspathUrlFinder.findClassPaths());
        } catch (Exception ex) {
            LOG.warn("Error while scanning for Autowired objects!", ex);
        }
    }

    /**
     * Get a map containing all objects annotated with {@link terrastore.annotation.AutoDetect} and of the given base type,
     * keyed by {@link terrastore.annotation.AutoDetect#name()}.
     *
     * @param type
     * @return
     */
    public final Map scanByType(Class type) {
        Map result = new HashMap<String, Object>();
        Set<String> autowiredObjects = annotations.getAnnotationIndex().get(AutoDetect.class.getName());
        if (autowiredObjects != null) {
            for (String name : autowiredObjects) {
                try {
                    Class candidateClass = Class.forName(name, false, Thread.currentThread().getContextClassLoader());
                    if (type.isAssignableFrom(candidateClass)) {
                        Object object = makeInstance(candidateClass);
                        AutoDetect annotation = object.getClass().getAnnotation(AutoDetect.class);
                        result.put(annotation.name(), object);
                    }
                } catch (Exception ex) {
                    LOG.warn("Error while scanning for Autowired objects of type: " + type, ex);
                }
            }
        }
        return result;
    }

    /**
     * Get a set containing all objects annotated with {@link terrastore.annotation.AutoDetect} and of the given base type,
     * ordered by {@link terrastore.annotation.AutoDetect#order()} (ascending).
     *
     * @param type
     * @return
     */
    public final SortedSet orderedScanByType(Class type) {
        Map annotatedObjects = scanByType(type);
        SortedSet orderedObjects = new TreeSet(new OrderComparator());
        orderedObjects.addAll(annotatedObjects.values());
        return orderedObjects;
    }

    private Object makeInstance(Class clazz) throws Exception {
        return clazz.newInstance();
    }

    private static class OrderComparator implements Comparator {

        @Override
        public int compare(Object o1, Object o2) {
            AutoDetect order1 = o1.getClass().getAnnotation(AutoDetect.class);
            AutoDetect order2 = o2.getClass().getAnnotation(AutoDetect.class);
            return Integer.valueOf(order1.order()).compareTo(Integer.valueOf(order2.order()));
        }
    }
}
