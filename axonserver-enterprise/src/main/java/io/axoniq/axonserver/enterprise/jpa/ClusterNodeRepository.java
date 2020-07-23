package io.axoniq.axonserver.enterprise.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * Provides access to {@link ClusterNode} entities.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public interface ClusterNodeRepository extends JpaRepository<ClusterNode, String> {

    /**
     * Deletes all {@link ClusterNode}s with name not equal to name.
     *
     * @param name the name
     */
    void deleteAllByNameNot(String name);

    /**
     * Retrieves a clusternode based on its internal hostname and grpc ports.
     *
     * @param internalHostName the hostname used for communication between axon server nodes
     * @param grpcInternalPort the grpc port used for communication between axon server nodes
     * @return a matching clusternode if found
     */
    Optional<ClusterNode> findFirstByInternalHostNameAndGrpcInternalPort(String internalHostName, int grpcInternalPort);
}
