package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventProvider;

public interface ContextEventIteratorFactory {

    EventProvider createFrom(String context);

}
