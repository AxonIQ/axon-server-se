package io.axoniq.axonserver.cluster.jpa;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Author: marc
 */
@Entity
public class JpaRaftState {

    @Id
    private String groupId;

    private long currentTerm;

    private String votedFor;

    private long commitIndex;

    private long lastApplied;

    public JpaRaftState() {
    }

    public JpaRaftState(String groupId) {
        this.groupId = groupId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }
}
