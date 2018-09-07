package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.jpa.ClusterNode;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */
public interface ClusterNodeRepository extends JpaRepository<ClusterNode, String> {
    ClusterNode findFirstByInternalHostNameAndGrpcInternalPort(String internalHostName, int internalPort);
}
