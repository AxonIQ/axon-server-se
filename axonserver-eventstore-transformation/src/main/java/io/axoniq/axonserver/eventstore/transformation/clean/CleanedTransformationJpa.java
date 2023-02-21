package io.axoniq.axonserver.eventstore.transformation.clean;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
@Table(name = "et_local_cleaned_event_store_transformation")
@Entity
public class CleanedTransformationJpa {

    @Id
    private String transformationId;

    public CleanedTransformationJpa() {
    }

    public CleanedTransformationJpa(String transformationId) {
        this.transformationId = transformationId;
    }

    public String transformationId() {
        return transformationId;
    }
}
