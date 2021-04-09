package io.axoniq.axonserver.refactoring.messaging.api;

import java.util.List;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface Error {

    String code();

    String message();

    List<String> details();

    String source();
}
