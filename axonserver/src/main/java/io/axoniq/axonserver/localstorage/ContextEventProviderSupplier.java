package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventProvider;

public interface ContextEventProviderSupplier {

    EventProvider eventProviderFor(String context); //TODO EventProvider should be moved to SE
}
