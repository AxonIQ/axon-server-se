package io.axoniq.axonhub.cluster;

import io.axoniq.axonhub.cluster.jpa.ClusterNode;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */
public interface ClusterNodeRepository extends JpaRepository<ClusterNode, String> {
    ClusterNode findFirstByInternalHostNameAndGrpcInternalPort(String internalHostName, int internalPort);
}
