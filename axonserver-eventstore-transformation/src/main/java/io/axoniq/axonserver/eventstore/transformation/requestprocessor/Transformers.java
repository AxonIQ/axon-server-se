package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

public interface Transformers {

    ContextTransformer transformerFor(String context);
}
