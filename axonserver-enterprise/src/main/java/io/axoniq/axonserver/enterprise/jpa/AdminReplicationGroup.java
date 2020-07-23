package io.axoniq.axonserver.enterprise.jpa;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.PrePersist;
import javax.persistence.PreRemove;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.UniqueConstraint;

/**
 * @author Marc Gathier
 */
@Entity
@Table(
        name = "adm_replication_group",
        uniqueConstraints = {@UniqueConstraint(columnNames = {"name"})})
public class AdminReplicationGroup {

    @Id
    private String replicationGroupId;

    private String name;


    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, mappedBy = "replicationGroup", fetch = FetchType.EAGER)
    private Set<AdminReplicationGroupMember> members = new HashSet<>();

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, mappedBy = "replicationGroup", fetch = FetchType.EAGER)
    private Set<AdminContext> contexts = new HashSet<>();
    @Column(name = "CHANGE_PENDING")
    private Boolean changePending;

    @Column(name = "PENDING_SINCE")
    @Temporal(TemporalType.TIMESTAMP)
    private Date pendingSince;

    public AdminReplicationGroup() {
    }

    public AdminReplicationGroup(String replicationGroupName) {
        this.name = replicationGroupName;
    }

    public String getReplicationGroupId() {
        return replicationGroupId;
    }

    public void setReplicationGroupId(String replicationGroupId) {
        this.replicationGroupId = replicationGroupId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<AdminReplicationGroupMember> getMembers() {
        return members;
    }

    public void setMembers(Set<AdminReplicationGroupMember> members) {
        this.members = members;
    }

    public void addMember(AdminReplicationGroupMember member) {
        member.setReplicationGroup(this);
        members.add(member);
    }

    public void removeMember(AdminReplicationGroupMember member) {
        member.setReplicationGroup(null);
        members.remove(member);
    }

    public Set<AdminContext> getContexts() {
        return contexts;
    }

    public void setContexts(Set<AdminContext> contexts) {
        this.contexts = contexts;
    }

    public boolean isChangePending() {
        return changePending != null && changePending;
    }

    public void setChangePending(boolean changePending) {
        this.changePending = changePending;
        if (changePending) {
            pendingSince = new Date();
        } else {
            pendingSince = null;
        }
    }

    public Date getPendingSince() {
        return pendingSince;
    }

    public void setPendingSince(Date pendingSince) {
        this.pendingSince = pendingSince;
    }

    public Collection<String> getMemberNames() {
        return members.stream().map(m -> m.getClusterNode().getName()).collect(Collectors.toSet());
    }

    public Optional<String> getNodeLabel(String node) {
        return getMember(node).map(AdminReplicationGroupMember::getClusterNodeLabel);
    }

    public Optional<AdminReplicationGroupMember> getMember(String node) {
        return members.stream()
                      .filter(m -> node.equals(m.getClusterNode().getName()))
                      .findFirst();
    }

    @PrePersist
    public void prePersist() {
        if (replicationGroupId == null) {
            replicationGroupId = UUID.randomUUID().toString();
        }
    }

    @PreRemove
    public void preRemove() {
        Set<AdminReplicationGroupMember> membersToRemove = new HashSet<>(members);
        membersToRemove.forEach(m -> m.getClusterNode().removeReplicationGroup(this));
        members.clear();
    }

    public AdminContext addContext(String contextName) {
        AdminContext context = new AdminContext(contextName);
        context.setReplicationGroup(this);
        contexts.add(context);
        return context;
    }

    public void removeContext(AdminContext context) {
        context.setReplicationGroup(null);
        this.contexts.remove(context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AdminReplicationGroup that = (AdminReplicationGroup) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    public void addContext(AdminContext context) {
        context.setReplicationGroup(this);
        contexts.add(context);
    }
}
