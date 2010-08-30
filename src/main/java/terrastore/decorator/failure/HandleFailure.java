/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package terrastore.decorator.failure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * @author sergio
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface HandleFailure {

    public Class exception();
}
