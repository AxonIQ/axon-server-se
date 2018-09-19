package io.axoniq.axonserver.migration.jpa;

import io.axoniq.axonserver.migration.BaseEvent;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Lob;
import javax.persistence.MappedSuperclass;

/**
 * Author: marc
 */
@MappedSuperclass
public class BaseEventEntry implements BaseEvent {
    @Column(
            nullable = false,
            unique = true
    )
    private String eventIdentifier;
    @Basic(
            optional = false
    )
    private String timeStamp;
    @Basic(
            optional = false
    )
    private String payloadType;
    @Basic
    private String payloadRevision;
    @Basic(
            optional = false
    )
    @Lob
    @Column(
            length = 10000
    )
    private byte[] payload;
    @Basic
    @Lob
    @Column(
            length = 10000
    )
    private byte[] metaData;


    public String getEventIdentifier() {
        return eventIdentifier;
    }

    public void setEventIdentifier(String eventIdentifier) {
        this.eventIdentifier = eventIdentifier;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public long getTimeStampAsLong() {
        if( timeStamp == null) return 0;
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(timeStamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        return zonedDateTime.toInstant().toEpochMilli();

    }


    public String getPayloadType() {
        return payloadType;
    }


    public String getPayloadRevision() {
        return payloadRevision;
    }


    public byte[] getPayload() {
        return payload;
    }


    public byte[] getMetaData() {
        return metaData;
    }

}
