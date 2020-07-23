package io.axoniq.axonserver.enterprise.replication.admin;

import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.UpdateLicense;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.junit.*;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link GrpcRaftConfigService}
 *
 * @author Sara Pellegrini
 * @since 4.1.1
 */
@RunWith(MockitoJUnitRunner.class)
public class GrpcRaftConfigServiceTest {

    private GrpcRaftConfigService service;
    private boolean isLeader = false;
    private LocalRaftConfigService localService;
    private RaftConfigService remoteLeaderService;

    @Before
    public void setUp() throws Exception {
        localService = mock(LocalRaftConfigService.class);
        remoteLeaderService = mock(RaftConfigService.class);
        service = new GrpcRaftConfigService(localService, () -> isLeader ? localService : remoteLeaderService);
    }

    @Test
    public void leaderReceiveJoinCluster() {
        isLeader = true;
        NodeInfo nodeInfo = NodeInfo.newBuilder().build();
        service.joinCluster(nodeInfo, fakeResponseObserver());
        verify(localService).join(nodeInfo);
    }

    @Test
    public void followerReceiveJoinCluster() {
        isLeader = false;
        NodeInfo nodeInfo = NodeInfo.newBuilder().build();
        service.joinCluster(nodeInfo, fakeResponseObserver());
        verify(remoteLeaderService).join(nodeInfo);
    }

    @NotNull
    private StreamObserver<UpdateLicense> fakeResponseObserver() {
        return new StreamObserver<UpdateLicense>() {
            @Override
            public void onNext(UpdateLicense value) {

            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };
    }


}
