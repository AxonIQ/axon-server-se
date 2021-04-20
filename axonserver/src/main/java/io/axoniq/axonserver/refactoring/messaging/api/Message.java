package io.axoniq.axonserver.refactoring.messaging.api;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface Message {

    String id();

    Optional<SerializedObject> payload();

    <T> T metadata(String key);

    Set<String> metadataKeys();

    default Map<String, Object> metadata() {
        return metadataKeys()
                .stream()
                .collect(Collectors.toMap(key -> key, this::metadata));
    }
}
