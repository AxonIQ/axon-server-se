package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

public interface TransformationEntry {

    long sequence();

    byte[] payload();

    byte version();
}
