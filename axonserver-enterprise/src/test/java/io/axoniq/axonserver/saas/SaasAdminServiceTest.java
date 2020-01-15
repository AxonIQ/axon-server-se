package io.axoniq.axonserver.saas;

import io.axoniq.axonserver.access.application.JpaApplicationRepository;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigService;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.internal.Context;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class SaasAdminServiceTest {

    private SaasAdminService testSubject;
    private List<ClusterNode> clusterNodeList = new ArrayList<>();
    private List<io.axoniq.axonserver.enterprise.jpa.Context> contextList = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        RaftConfigService raftConfigService = mock(RaftConfigService.class);

        ClusterController clusterController = mock(ClusterController.class);
        ContextController contextController = mock(ContextController.class);

        JpaApplicationRepository applicationRepository = mock(JpaApplicationRepository.class);
        testSubject = new SaasAdminService(
                () -> clusterNodeList.stream(),
                () -> contextList.stream(),
                () -> Collections.emptyList(),
                () -> raftConfigService
        );
    }

    @Test
    public void createContextWithInvalidName() throws InterruptedException, TimeoutException {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        testSubject.createContext(Context.newBuilder()
                                         .setName("Demo/demo")
                                         .build(), new StreamObserver<InstructionAck>() {
            @Override
            public void onNext(InstructionAck instructionAck) {
                completableFuture.complete(instructionAck);
            }

            @Override
            public void onError(Throwable throwable) {
                completableFuture.completeExceptionally(throwable);
            }

            @Override
            public void onCompleted() {

            }
        });

        try {
            completableFuture.get(1, TimeUnit.SECONDS);
            fail("Invalid context name must fail");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof StatusRuntimeException);
            MessagingPlatformException mpe = GrpcExceptionBuilder.parse(ex.getCause());
            assertEquals(ErrorCode.INVALID_CONTEXT_NAME, mpe.getErrorCode());
        }
    }
}