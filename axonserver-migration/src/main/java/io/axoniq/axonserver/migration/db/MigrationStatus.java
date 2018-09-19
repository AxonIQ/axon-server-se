package io.axoniq.axonserver.migration.db;

import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Author: marc
 */
@Entity
@Cacheable(false)
public class MigrationStatus {
    @Id
    private long id = 1;

    private long lastEventGlobalIndex = -1;
    private String lastSnapshotTimestamp = "1970-01-01T00:00:00Z";
    private String lastSnapshotEventId;

    public MigrationStatus() {
    }


    public long getId() {
        return id;
    }


    public long getLastEventGlobalIndex() {
        return lastEventGlobalIndex;
    }

    public void setLastEventGlobalIndex(long lastEventGlobalIndex) {
        this.lastEventGlobalIndex = lastEventGlobalIndex;
    }

    public String getLastSnapshotTimestamp() {
        return lastSnapshotTimestamp;
    }

    public void setLastSnapshotTimestamp(String lastSnapshotTimestamp) {
        this.lastSnapshotTimestamp = lastSnapshotTimestamp;
    }

    public String getLastSnapshotEventId() {
        return lastSnapshotEventId;
    }

    public void setLastSnapshotEventId(String lastSnapshotEventId) {
        this.lastSnapshotEventId = lastSnapshotEventId;
    }
}
