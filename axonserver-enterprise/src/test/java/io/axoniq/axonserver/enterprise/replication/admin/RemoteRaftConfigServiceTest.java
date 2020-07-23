package io.axoniq.axonserver.enterprise.replication.admin;

import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ContextNames;
import io.axoniq.axonserver.grpc.internal.DeleteContextRequest;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeName;
import io.axoniq.axonserver.grpc.internal.NodeReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.RaftConfigServiceGrpc;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupContext;
import io.axoniq.axonserver.grpc.internal.UpdateLicense;
import io.axoniq.axonserver.grpc.internal.User;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Marc Gathier
 */
public class RemoteRaftConfigServiceTest {

    private static final String CONTEXT = "TEST";
    private static final String NODE = "NODE";
    private static final int port = 27777;
    private RemoteRaftConfigService testSubject;
    private static Server dummyServer;

    @BeforeClass
    public static void startServer() throws IOException {
        dummyServer = ServerBuilder.forPort(port)
                                   .addService(new DummyRemoteRaftConfigServiceGrpc())
                                   .build();
        dummyServer.start();
    }

    @AfterClass
    public static void stopServer() throws InterruptedException {
        if (dummyServer != null && !dummyServer.isShutdown()) {
            dummyServer.shutdownNow().awaitTermination();
        }
    }

    @Before
    public void setUp() {
        Channel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        testSubject = new RemoteRaftConfigService(RaftConfigServiceGrpc.newStub(channel));
    }

    @Test
    public void addNodeToContext() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.addNodeToReplicationGroup(CONTEXT, NODE, Role.PRIMARY).get(1, TimeUnit.SECONDS);
    }

    @Test
    public void deleteNode() {
        testSubject.deleteNode(NODE);
    }

    @Test
    public void deleteNodeIfEmpty() {
        testSubject.deleteNode(NODE);
    }

    @Test
    public void deleteContext() {
        testSubject.deleteContext(CONTEXT, false);
    }

    @Test
    public void deleteNodeFromContext() {
        testSubject.deleteNodeFromReplicationGroup(CONTEXT, NODE);
    }

    @Test
    public void addContext() {
        testSubject.addContext("demo", "democontext", Collections.emptyMap());
    }

    @Test
    public void join() {
        testSubject.join(NodeInfo.newBuilder().build());
    }

    @Test
    public void init() {
        testSubject.init(Collections.singletonList(CONTEXT));
    }

    @Test
    public void updateApplication() {
        testSubject.updateApplication(Application.getDefaultInstance());
    }

    @Test
    public void refreshToken() {
        testSubject.refreshToken(Application.getDefaultInstance());
    }

    @Test
    public void updateUser() {
        testSubject.updateUser(User.getDefaultInstance());
    }

    @Test
    public void updateProcessorLoadBalancing() {
        testSubject.updateProcessorLoadBalancing(ProcessorLBStrategy.getDefaultInstance());
    }

    @Test
    public void deleteUser() {
        testSubject.deleteUser(User.getDefaultInstance());
    }

    @Test
    public void deleteApplication() {
        testSubject.deleteApplication(Application.getDefaultInstance());
    }

    private static class DummyRemoteRaftConfigServiceGrpc extends RaftConfigServiceGrpc.RaftConfigServiceImplBase {

        @Override
        public void initCluster(ContextNames request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void joinCluster(NodeInfo request, StreamObserver<UpdateLicense> responseObserver) {
            responseObserver.onNext(UpdateLicense.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void deleteNode(NodeName request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void createContext(ReplicationGroupContext request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void addNodeToReplicationGroup(NodeReplicationGroup request,
                                              StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void deleteNodeFromReplicationGroup(NodeReplicationGroup request,
                                                   StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void deleteContext(DeleteContextRequest request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void updateApplication(Application request, StreamObserver<Application> responseObserver) {
            responseObserver.onNext(Application.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void deleteApplication(Application request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void refreshToken(Application request, StreamObserver<Application> responseObserver) {
            responseObserver.onNext(Application.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void updateUser(User request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void deleteUser(User request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void updateProcessorLBStrategy(ProcessorLBStrategy request,
                                              StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void deleteProcessorLBStrategy(ProcessorLBStrategy request,
                                              StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }
    }
}