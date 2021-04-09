package io.axoniq.axonserver.refactoring.messaging.api;

import java.util.Set;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface Message {

    String id();

    Object payload();

    <T> T metadata(String key);

    Set<String> metadataKeys();
}
