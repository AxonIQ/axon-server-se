package io.axoniq.axonserver.commandprocessing.spi;

import java.io.Serializable;
import java.util.Optional;

public interface Metadata extends Serializable {


    Iterable<String> metadataKeys();

    <R extends Serializable> Optional<R> metadataValue(String metadataKey);

    default <R extends Serializable> R metadataValue(String metadataKey, R defaultValue) {
        //noinspection unchecked
        return (R) metadataValue(metadataKey).orElse(defaultValue);
    }

    static boolean isInternal(String key) {
        return key.startsWith("__");
    }
}
