package io.axoniq.axonserver.localstorage;

import java.util.function.Consumer;

public interface LocalEventStoreInitializationObservable {

    void accept(Consumer<String> callback);
}
