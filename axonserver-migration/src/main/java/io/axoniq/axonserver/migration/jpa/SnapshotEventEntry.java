package io.axoniq.axonserver.migration.jpa;

import io.axoniq.axonserver.migration.SnapshotEvent;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Objects;

/**
 * @author Marc Gathier
 */
@Entity
@NamedQuery(name = "SnapshotEventEntry.findByTimestamp", query = "select e from SnapshotEventEntry e where e.timeStamp >= :lastTimeStamp order by e.timeStamp asc, e.eventIdentifier")
@IdClass(SnapshotEventEntry.PK.class)
public class SnapshotEventEntry extends BaseEventEntry implements SnapshotEvent {

    @Id
    private String aggregateIdentifier;
    @Id
    private long sequenceNumber;
    @Id
    private String type;

    @Override
    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String getType() {
        return type;
    }


    public static class PK implements Serializable {
        private static final long serialVersionUID = 9182347799552520594L;

        private String aggregateIdentifier;
        private long sequenceNumber;
        private String type;



        /**
         * Constructor for JPA. Not to be used directly
         */
        public PK() {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SnapshotEventEntry.PK pk = (SnapshotEventEntry.PK) o;
            return sequenceNumber == pk.sequenceNumber && Objects.equals(aggregateIdentifier, pk.aggregateIdentifier) &&
                    Objects.equals(type, pk.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aggregateIdentifier, type, sequenceNumber);
        }

        @Override
        public String toString() {
            return "PK{type='" + type + '\'' + ", aggregateIdentifier='" + aggregateIdentifier + '\'' +
                    ", sequenceNumber=" + sequenceNumber + '}';
        }
    }
}
