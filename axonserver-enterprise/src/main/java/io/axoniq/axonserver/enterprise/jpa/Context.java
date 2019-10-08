package io.axoniq.axonserver.enterprise.jpa;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.PreRemove;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * @author Marc Gathier
 */
@Entity(name = "Context")
public class Context implements Serializable {
    @Id
    private String name;

    @OneToMany(fetch = FetchType.EAGER, mappedBy = "key.context", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<ContextClusterNode> nodes = new HashSet<>();

    @Column(name="CHANGE_PENDING")
    private Boolean changePending;

    @Column(name="PENDING_SINCE")
    @Temporal(TemporalType.TIMESTAMP)
    private Date pendingSince;

    public Context() {
    }

    public Context(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<ContextClusterNode> getNodes() {
        return nodes;
    }

    public Collection<String> getNodeNames() {
        return nodes.stream().map(t -> t.getClusterNode().getName()).collect(Collectors.toSet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Context context1 = (Context) o;
        return Objects.equals(name, context1.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @PreRemove
    public void clearContexts() {
        nodes.forEach(ccn -> ccn.getClusterNode().remove(ccn));
        nodes.clear();
    }

    public void remove(ContextClusterNode ccn) {
        nodes.remove(ccn);
    }

    public Set<ContextClusterNode> getAllNodes() {
        return nodes;
    }

    public void addClusterNode(ContextClusterNode contextClusterNode) {
        nodes.add(contextClusterNode);
    }

    public boolean isChangePending() {
        return changePending != null && changePending;
    }

    public void changePending(Boolean changePending) {
        this.changePending = changePending;
        if(changePending != null && changePending) {
            pendingSince = new Date();
        } else {
            pendingSince = null;
        }
    }

    public Date getPendingSince() {
        return pendingSince;
    }

    @Override
    public String toString() {
        return "Context{" +
                "name='" + name + '\'' +
                '}';
    }

    public String getNodeLabel(String node) {
        return nodes.stream().filter(n -> n.getClusterNode().getName().equals(node))
                    .map(ContextClusterNode::getClusterNodeLabel)
                    .findFirst().orElse(null);
    }
}
