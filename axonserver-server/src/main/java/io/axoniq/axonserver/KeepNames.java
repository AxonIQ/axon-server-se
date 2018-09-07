package io.axoniq.axonserver;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Author: marc
 * Annotation for obfuscator to indicate that for this class the class and method names should be kept.
 */
@Inherited
@Target(ElementType.TYPE)
@Retention(value=RUNTIME)
public @interface KeepNames {
}
