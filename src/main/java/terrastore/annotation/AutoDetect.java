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
package terrastore.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotate components that needs to be auto-detected and wired into Terrastore server configuration.<br>
 * Classes implementing the following interfaces can be marked as auto-detected components:
 * <ul>
 * <li>{@link terrastore.event.EventListener}</li>
 * <li>{@link terrastore.store.operators.Comparator}</li>
 * <li>{@link terrastore.store.operators.Condition}</li>
 * <li>{@link terrastore.store.operators.Function}</li>
 * </ul>
 * Classes marked with this annotation must have a no-arg constructor in order to be created and
 * wired into Terrastore configuration.
 *
 * @author Sergio Bossa
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface AutoDetect {

    /**
     * The name of the autodetected component.
     */
    public String name();

    /**
     * The wiring precedence (optional): lower order means higher precedence.
     */
    public int order() default 0;
}
