package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

public interface ContextEventProviderSupplier {

    EventProvider eventProviderFor(String context);
}
