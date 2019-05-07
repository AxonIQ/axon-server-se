package io.axoniq.axonserver.enterprise.jpa;

import io.axoniq.axonserver.RaftAdminGroup;
import io.axoniq.axonserver.grpc.cluster.Node;
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
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.PreRemove;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * @author Marc Gathier
 */
@Entity
@Table(uniqueConstraints={@UniqueConstraint(columnNames = {"internalHostName", "grpcInternalPort"})})
@NamedQueries(
        {
                @NamedQuery(name = "ClusterNode.findAll", query = "select c from ClusterNode c"),
                @NamedQuery(name = "ClusterNode.findByInternalHostNameAndPort", query=" select c from ClusterNode c where c.internalHostName = :internalHostName and c.grpcInternalPort = :internalPort")
        }
)
public class ClusterNode implements Serializable, AxonServerNode {

    @Id
    private String name;

    private String hostName;
    private String internalHostName;
    private Integer grpcPort;
    private Integer grpcInternalPort;
    private Integer httpPort;

    @Transient
    private Map<String,String> tags = new HashMap<>();

    @OneToMany(mappedBy = "key.clusterNode", fetch = FetchType.EAGER, cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<ContextClusterNode> contexts = new HashSet<>();


    public ClusterNode() {
    }

    public ClusterNode(String name, String hostName, String internalHostName, Integer grpcPort, Integer grpcInternalPort, Integer httpPort) {
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

    public String getInternalHostName() {
        return internalHostName;
    }

    public String getName() {
        return name;
    }

    /**
     * @param tags the tags that are configured for this node
     */
    public void setTags(Map<String,String> tags){
        this.tags=tags;
    }

    /**
     * @return the tags configured for this node
     */
    public Map<String,String> getTags(){
        return tags;
    }

    public Collection<String> getContextNames() {
        return contexts.stream().map(ccn -> ccn.getContext().getName()).collect(Collectors.toSet());
    }

    @Override
    public Collection<String> getStorageContextNames() {
        return contexts.stream().map(ccn -> ccn.getContext().getName())
                       .filter(n -> !RaftAdminGroup.isAdmin(n))
                       .collect(Collectors.toSet());
    }

    public void setInternalHostName(String internalHostName) {
        this.internalHostName = internalHostName;
    }

    public void setGrpcInternalPort(Integer grpcInternalPort) {
        this.grpcInternalPort = grpcInternalPort;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void addContext(Context context, String clusterNodeLabel) {
        ContextClusterNode contextClusterNode = contexts.stream()
                                                        .filter(ccn -> ccn.getContext().equals(context))
                                                        .findFirst()
                                                        .orElse(new ContextClusterNode(context, this, clusterNodeLabel));
    }

    public static ClusterNode from(NodeInfo connect) {
        return new ClusterNode(connect.getNodeName(), connect.getHostName(), connect.getInternalHostName(), connect.getGrpcPort(), connect.getGrpcInternalPort(), connect.getHttpPort());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

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
        Optional<ContextClusterNode> contextClusterNode = contexts.stream().filter(ccn -> context.equals(ccn.getContext().getName())).findFirst();
        contextClusterNode.ifPresent(ContextClusterNode::preDelete);
    }

    public boolean isAdmin() {
        return getContextNames().contains(getAdmin());
    }
}
