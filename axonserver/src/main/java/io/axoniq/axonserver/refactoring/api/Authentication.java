package io.axoniq.axonserver.refactoring.api;

import java.util.Set;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface Authentication {

    String name();

    Set<String> roles();

    String detail(String key);

    Set<String> detailsKeys();
}
