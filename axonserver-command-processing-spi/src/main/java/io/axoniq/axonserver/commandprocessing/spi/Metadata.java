package io.axoniq.axonserver.commandprocessing.spi;

import java.io.Serializable;
import java.util.Optional;

public interface Metadata {


    Iterable<String> metadataKeys();

    <R extends Serializable> Optional<R> metadataValue(String metadataKey);

    default <R extends Serializable> R metadataValue(String metadataKey, R defaultValue) {
        return (R) metadataValue(metadataKey).orElse(defaultValue);
    }

    static boolean isInternal(String key) {
        return key.startsWith("__");
    }
}
