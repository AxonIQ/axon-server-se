package io.axoniq.axonserver.localstorage.jdbc;

import com.google.protobuf.ByteString;
import io.axoniq.axondb.Event;
import io.axoniq.platform.SerializedObject;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.MappedSuperclass;

/**
 * Author: marc
 */
@MappedSuperclass
public abstract class AbstractSequencedDomainEventEntry {
    @Id
    private long globalIndex;

    @Basic(optional = false)
    private String aggregateIdentifier;
    @Basic(optional = false)
    private long sequenceNumber;
    @Basic(optional = false)
    private String type;

    @Column(nullable = false, unique = true)
    private String eventIdentifier;
    @Basic(optional = false)
    private long timeStamp;
    @Basic(optional = false)
    private String payloadType;
    @Basic
    private String payloadRevision;

    @Basic(optional = false)
    @Lob
    @Column(length = 10000)
    private byte[] payload;
    @Basic
    @Lob
    @Column(length = 10000)
    private byte[] metaData;

    public AbstractSequencedDomainEventEntry(Event eventMessage) {
        this.aggregateIdentifier = eventMessage.getAggregateIdentifier();
        this.type = eventMessage.getAggregateType();
        this.eventIdentifier = eventMessage.getMessageIdentifier();
        // TODO: this.metaData
        this.payload = eventMessage.getPayload().getData().toByteArray();
        this.payloadRevision = eventMessage.getPayload().getRevision();
        this.payloadType = eventMessage.getPayload().getType();
        this.sequenceNumber = eventMessage.getAggregateSequenceNumber();
        this.timeStamp = eventMessage.getTimestamp();
    }

    protected AbstractSequencedDomainEventEntry() {
    }

    public long getGlobalIndex() {
        return globalIndex;
    }

    public void setGlobalIndex(long globalIndex) {
        this.globalIndex = globalIndex;
    }

    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    public void setAggregateIdentifier(String aggregateIdentifier) {
        this.aggregateIdentifier = aggregateIdentifier;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getEventIdentifier() {
        return eventIdentifier;
    }

    public void setEventIdentifier(String eventIdentifier) {
        this.eventIdentifier = eventIdentifier;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getPayloadType() {
        return payloadType;
    }

    public void setPayloadType(String payloadType) {
        this.payloadType = payloadType;
    }

    public String getPayloadRevision() {
        return payloadRevision;
    }

    public void setPayloadRevision(String payloadRevision) {
        this.payloadRevision = payloadRevision;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public byte[] getMetaData() {
        return metaData;
    }

    public void setMetaData(byte[] metaData) {
        this.metaData = metaData;
    }

    public abstract Event asEvent();

    protected Event.Builder prepareEvent() {
        return Event.newBuilder()
                    .setAggregateIdentifier(aggregateIdentifier)
                    .setAggregateSequenceNumber(sequenceNumber)
                    .setMessageIdentifier(eventIdentifier)
                    .setPayload(SerializedObject.newBuilder()
                                                .setData(ByteString.copyFrom(payload))
                                                .setRevision(payloadRevision)
                                                .setType(payloadType))
                    .setTimestamp(timeStamp)
                    .setAggregateType(type);
    }

}
