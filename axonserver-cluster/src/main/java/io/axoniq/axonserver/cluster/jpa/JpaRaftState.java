package io.axoniq.axonserver.cluster.jpa;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Marc Gathier
 * @since 4.1
 */
@Entity
@Table(name = "rg_state")
public class JpaRaftState {

    @Id
    private String groupId;

    private long currentTerm;

    private String votedFor;

    private long commitIndex;

    private long commitTerm;

    private long lastAppliedIndex;

    private long lastAppliedTerm;

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

    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    public void setLastAppliedIndex(long lastAppliedIndex) {
        this.lastAppliedIndex = lastAppliedIndex;
    }

    public long commitTerm() {
        return commitTerm;
    }

    public void setCommitTerm(long commitTerm) {
        this.commitTerm = commitTerm;
    }

    public long lastAppliedTerm() {
        return lastAppliedTerm;
    }

    public void setLastAppliedTerm(long lastAppliedTerm) {
        this.lastAppliedTerm = lastAppliedTerm;
    }
}
