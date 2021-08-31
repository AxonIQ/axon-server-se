package io.axoniq.axonserver.requestprocessor.eventstore;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Entity
@Table(name = "event_store_transformations")
public class EventStoreTransformationJpa {

    public enum Status {
        CREATED,
        APPLYING,
        APPLIED
    }
    @Id
    private String transformationId;
    private String context;
    private Long firstToken;
    private Long lastToken;

    @Enumerated(EnumType.ORDINAL)
    private Status status;

    public EventStoreTransformationJpa(String transformationId, String context) {
        this.transformationId = transformationId;
        this.context = context;
        this.status = Status.CREATED;
    }

    public EventStoreTransformationJpa() {
    }

    public String getTransformationId() {
        return transformationId;
    }

    public void setTransformationId(String transformationId) {
        this.transformationId = transformationId;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public void setLastToken(Long lastToken) {
        if( firstToken == null) {
            firstToken = lastToken;
        }
        this.lastToken = lastToken;
    }

    public Long getLastToken() {
        return lastToken;
    }

    public Long getFirstToken() {
        return firstToken;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }


}
