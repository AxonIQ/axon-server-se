package io.axoniq.axonserver.enterprise.jpa;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.PreRemove;

/**
 * @author Marc Gathier
 */
@Entity(name = "Context")
public class Context implements Serializable {
    @Id
    private String name;

    @OneToMany(fetch = FetchType.EAGER, mappedBy = "key.context", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<ContextClusterNode> nodes = new HashSet<>();

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

    public Set<ClusterNode> getStorageNodes() {
        return nodes.stream().filter(ContextClusterNode::isStorage).map(ContextClusterNode::getClusterNode).collect(Collectors.toSet());
    }
    public Set<ClusterNode> getMessagingNodes() {
        return nodes.stream().filter(ContextClusterNode::isMessaging).map(ContextClusterNode::getClusterNode).collect(Collectors.toSet());
    }

    public boolean isMessagingMember(String nodeName) {
        return nodes.stream().anyMatch(n -> n.getClusterNode().getName().equals(nodeName));
    }

    public boolean isStorageMember(String nodeName) {
        return nodes.stream().filter(ContextClusterNode::isStorage).anyMatch(n -> n.getClusterNode().getName().equals(nodeName));
    }


    public ContextClusterNode getMember(String node) {
        return nodes.stream().filter(n -> n.getClusterNode().getName().equals(node)).findFirst().orElse(null);
    }

    public Collection<String> getStorageNodeNames() {
        return nodes.stream().filter(ContextClusterNode::isStorage).map(t -> t.getClusterNode().getName()).collect(Collectors.toSet());
    }

    public Collection<String> getMessagingNodeNames() {
        return nodes.stream().filter(ContextClusterNode::isMessaging).map(t -> t.getClusterNode().getName()).collect(Collectors.toSet());
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

    @Override
    public String toString() {
        return "Context{" +
                "name='" + name + '\'' +
                '}';
    }
}
