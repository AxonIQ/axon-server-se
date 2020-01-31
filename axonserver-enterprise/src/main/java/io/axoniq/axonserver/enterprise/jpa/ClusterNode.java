package io.axoniq.axonserver.enterprise.jpa;

import io.axoniq.axonserver.RaftAdminGroup;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.topology.AxonServerNode;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.PreRemove;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * Stores cluster nodes in the controldb.
 * @author Marc Gathier
 * @since 4.0
 */
@Entity
@Table(uniqueConstraints = {@UniqueConstraint(columnNames = {"internalHostName", "grpcInternalPort"})})
public class ClusterNode implements Serializable, AxonServerNode {

    @Id
    private String name;

    private String hostName;
    private String internalHostName;
    private Integer grpcPort;
    private Integer grpcInternalPort;
    private Integer httpPort;

    @Transient
    private Map<String, String> tags = new HashMap<>();

    @OneToMany(mappedBy = "key.clusterNode", fetch = FetchType.EAGER, cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<ContextClusterNode> contexts = new HashSet<>();


    public ClusterNode() {
    }

    public ClusterNode(String name, String hostName, String internalHostName, Integer grpcPort,
                       Integer grpcInternalPort, Integer httpPort) {
        this.hostName = hostName;
        this.internalHostName = internalHostName;
        this.grpcPort = grpcPort;
        this.grpcInternalPort = grpcInternalPort;
        this.httpPort = httpPort;
        this.name = name;
    }

    public ClusterNode(Node node) {
        this.internalHostName = node.getHost();
        this.grpcInternalPort = node.getPort();
        this.name = node.getNodeName();
    }

    public static ClusterNode from(NodeInfo connect) {
        return new ClusterNode(connect.getNodeName(),
                               connect.getHostName(),
                               connect.getInternalHostName(),
                               connect.getGrpcPort(),
                               connect.getGrpcInternalPort(),
                               connect.getHttpPort());
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public Integer getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(Integer grpcPort) {
        this.grpcPort = grpcPort;
    }

    public Integer getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(Integer httpPort) {
        this.httpPort = httpPort;
    }

    public Integer getGrpcInternalPort() {
        return grpcInternalPort;
    }

    public void setGrpcInternalPort(Integer grpcInternalPort) {
        this.grpcInternalPort = grpcInternalPort;
    }

    public String getInternalHostName() {
        return internalHostName;
    }

    public void setInternalHostName(String internalHostName) {
        this.internalHostName = internalHostName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the tags configured for this node
     */
    public Map<String, String> getTags() {
        return tags;
    }

    /**
     * @param tags the tags that are configured for this node
     */
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Collection<String> getContextNames() {
        return contexts.stream().map(ccn -> ccn.getContext().getName()).collect(Collectors.toSet());
    }

    /**
     * Return the names of contexts that store event data on this node. Excludes contexts that are admin contexts or
     * have
     * role messaging-only.
     *
     * @return names of contexts that store event data on this node
     */
    @Override
    public Collection<String> getStorageContextNames() {
        return contexts.stream()
                       .filter(ccn -> RoleUtils.hasStorage(ccn.getRole()))
                       .map(ccn -> ccn.getContext().getName())
                       .filter(n -> !RaftAdminGroup.isAdmin(n))
                       .collect(Collectors.toSet());
    }

    /**
     * Adds this node the specified context with given label and role. If this node is already a member of the context
     * this is a no-op.
     *
     * @param context          the context where to add the node to
     * @param clusterNodeLabel unique label for the node
     * @param role             the role of this node in the context
     */
    public void addContext(Context context, String clusterNodeLabel, Role role) {
        if (!getContext(context.getName()).isPresent()) {
            ContextClusterNode newMember = new ContextClusterNode(context, this, clusterNodeLabel, role);
            addContext(newMember);
            context.addClusterNode(newMember);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClusterNode that = (ClusterNode) o;

        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public NodeInfo toNodeInfo() {
        return NodeInfo.newBuilder()
                       .setNodeName(name)
                       .setGrpcInternalPort(grpcInternalPort)
                       .setGrpcPort(grpcPort)
                       .setHostName(hostName)
                       .setInternalHostName(internalHostName)
                       .setHttpPort(httpPort)
                       .putAllTags(tags)
                       .setVersion(1)
                       .build();
    }

    @PreRemove
    public void clearContexts() {
        contexts.forEach(ccn -> ccn.getContext().remove(ccn));
        contexts.clear();
    }

    public void remove(ContextClusterNode ccn) {
        contexts.remove(ccn);
    }

    public void addContext(ContextClusterNode contextClusterNode) {
        contexts.add(contextClusterNode);
    }

    public Set<ContextClusterNode> getContexts() {
        return contexts;
    }

    public void removeContext(String context) {
        getContext(context).ifPresent(ContextClusterNode::preDelete);
    }

    public boolean isAdmin() {
        return getContextNames().contains(getAdmin());
    }

    public Optional<ContextClusterNode> getContext(String contextName) {
        return getContexts().stream()
                            .filter(c -> c.getContext().getName().equals(contextName))
                            .findFirst();
    }
}
