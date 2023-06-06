package io.axoniq.axonserver.eventstore.transformation.clean;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface TransformationIdentifier {

    String context();

    String id();
}
