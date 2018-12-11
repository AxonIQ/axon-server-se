package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.ManagedChannelHelper;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.internal.RaftConfigServiceGrpc;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import org.springframework.stereotype.Component;

import java.util.function.Function;

/**
 * Author: marc
 */
@Component
public class RaftServiceFactory  {

    private final MessagingPlatformConfiguration configuration;
    private final ClusterController clusterController;
    private Function<String,String> contextLeaderProvider;
    private RaftGroupService localRaftGroupService;
    private RaftConfigService localRaftConfigService;

    public RaftServiceFactory(MessagingPlatformConfiguration configuration,
                              ClusterController clusterController
                              ) {
        this.configuration = configuration;
        this.clusterController = clusterController;
    }

    public void setContextLeaderProvider(Function<String, String> contextLeaderProvider) {
        this.contextLeaderProvider = contextLeaderProvider;
    }

    public void setLocalRaftGroupService(
            RaftGroupService localRaftGroupService) {
        this.localRaftGroupService = localRaftGroupService;
    }

    public void setLocalRaftConfigService(RaftConfigService localRaftConfigService) {
        this.localRaftConfigService = localRaftConfigService;
    }

    public RaftGroupService getRaftGroupService(String groupId) {
        String leader = contextLeaderProvider.apply(groupId);
        if( configuration.getName().equals(leader)) return localRaftGroupService;

        ClusterNode node = clusterController.getNode(leader);
        return new RemoteRaftGroupService(RaftGroupServiceGrpc.newStub(ManagedChannelHelper
                .createManagedChannel(configuration, node)));
    }

    public RaftConfigService getRaftConfigService() {
        String leader = contextLeaderProvider.apply(GrpcRaftController.ADMIN_GROUP);
        if( configuration.getName().equals(leader)) return localRaftConfigService;

        ClusterNode node = clusterController.getNode(leader);
        return new RemoteRaftConfigService(RaftConfigServiceGrpc.newStub(ManagedChannelHelper
                                                                               .createManagedChannel(configuration, node)));
    }

    public RaftConfigServiceGrpc.RaftConfigServiceBlockingStub getRaftConfigServiceStub(String host, int port) {
        return RaftConfigServiceGrpc.newBlockingStub(ManagedChannelHelper.createManagedChannel(configuration, host, port));
    }

    public RaftGroupService getRaftGroupServiceForNode(String nodeName) {
        if( configuration.getName().equals(nodeName)) return localRaftGroupService;

        ClusterNode node = clusterController.getNode(nodeName);
        return new RemoteRaftGroupService( RaftGroupServiceGrpc.newStub(ManagedChannelHelper
                                                                                        .createManagedChannel(configuration, node)));
    }

    public RaftConfigService getLocalRaftConfigService() {
        return localRaftConfigService;
    }
}
