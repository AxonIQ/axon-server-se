package io.axoniq.axonserver.eventstore.transformation.clean;

import org.springframework.data.repository.CrudRepository;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface CleanedTransformationRepository extends CrudRepository<CleanedTransformationJpa, String> {

}
